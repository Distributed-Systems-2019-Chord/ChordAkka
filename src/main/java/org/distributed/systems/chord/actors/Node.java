package org.distributed.systems.chord.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.dispatch.OnComplete;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.io.Tcp;
import akka.io.Tcp.CommandFailed;
import akka.io.Tcp.Connected;
import akka.io.TcpMessage;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.typesafe.config.Config;
import org.distributed.systems.ChordStart;
import org.distributed.systems.chord.messaging.*;
import org.distributed.systems.chord.util.impl.HashUtil;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.time.Duration;

public class Node extends AbstractActor {

    public class FingerTableEntry {

        public long id;

        // Is IP + Port
        public ActorRef chordRef;

        @Override
        public String toString() {
            return "ID " + id + " of " + chordRef.toString();
        }
    }

    private FingerTableEntry[] fingerTable;

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    public static final int m = 6; // Number of bits in key id's
    public static final long AMOUNT_OF_KEYS = Math.round(Math.pow(2, m));
    static final int MEMCACHE_MIN_PORT = 11211;
    static final int MEMCACHE_MAX_PORT = 12235;
    final ActorRef manager;

    private ActorRef storageActorRef;
    private Config config = getContext().getSystem().settings().config();

    private ActorRef predecessor = null;
    private long predecessorId;

    private String type = "";
    private long id;
    private Thread ticker;
    private Thread fix_fingers_ticker;
    private String lastTickerOutput = "";
    private int fix_fingers_next = 0;

    public Node() {
        this.manager = Tcp.get(getContext().getSystem()).manager();
        this.storageActorRef = getContext().actorOf(Props.create(StorageActor.class));
        this.type = getNodeType();
    }

    public static Props props(ActorRef manager) {
        return Props.create(Node.class, manager);
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        log.info("Starting up...     ref: " + getSelf());
        this.id = getNodeId();

        // Init the FingerTable
        fingerTable = new FingerTableEntry[Node.m];

        if (this.type.equals("central")) {
            this.predecessor = null;
            this.predecessorId = -1;
            this.setSuccessor(getSelf(), this.id);
            System.out.println("Bootstrapped Central Node");
            getSelf().tell(new Stabelize.Request(), getSelf());
        } else {
            final String centralNodeAddress = getCentralNodeAddress();
            Timeout timeout = Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT));
            Future<ActorRef> centralNodeFuture = getContext().actorSelection(centralNodeAddress).resolveOne(timeout);
            ActorRef centralNode = (ActorRef) Await.result(centralNodeFuture, timeout.duration());
            JoinMessage.JoinRequest joinRequestMessage = new JoinMessage.JoinRequest(centralNode, this.id);
            System.out.println("Bootstrapped Regular Node");
            getSelf().tell(joinRequestMessage, getSelf());
        }

        this.ticker = new Thread(() -> {


            //Do whatever
            while (true) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                StringBuilder sb = new StringBuilder();
                sb.append("----------------------------------------------------------------------\n");
                sb.append("S: " + +this.fingerTable[0].id + " ActorRef:" + this.fingerTable[0].chordRef + "\n");
                sb.append("P: " + this.predecessorId + " ActorRef: " + this.predecessor + "\n");
                sb.append(toStringFingerTable());
                sb.append("----------------------------------------------------------------------\n");

                if (!this.lastTickerOutput.equals(sb.toString())) {
                    System.out.println(sb.toString());
                    this.lastTickerOutput = sb.toString();
                }

                getSelf().tell(new Stabelize.Request(), getSelf());
            }
        });

        this.fix_fingers_ticker = new Thread(() -> {
            // Fixing Finger Tables
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                getSelf().tell(new FixFingers.Request(), getSelf());
            }
        });
        this.ticker.start();
        this.fix_fingers_ticker.start();
    }

    boolean checkOverZero(long left, long right, long value) {
        return ((left < value && value > right) || (left > value && value < right)) && right <= left;
    }

    boolean checkInBetweenNotOverZero(long left, long right, long value) {
        return left < value && value < right;
    }

    boolean isBetweenExeclusive(long left, long right, long value) {
        return checkOverZero(left, right, value) || checkInBetweenNotOverZero(left, right, value);
    }

    @Override
    public Receive createReceive() {
        log.info("Received a message");

        return receiveBuilder()
                .match(JoinMessage.JoinRequest.class, msg -> {
                    System.out.println("Node " + this.id + " wants to join");

                    Timeout timeout = Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT));
                    Future<Object> centralNodeFuture = Patterns.ask(msg.requestor, new FindSuccessor.Request(this.id, 0), timeout);
                    FindSuccessor.Reply rply = (FindSuccessor.Reply) Await.result(centralNodeFuture, timeout.duration());

                    this.setSuccessor(rply.succesor, rply.id);
                    System.out.println("Node " + this.id + "joined! ");
                    System.out.println("S: " + this.fingerTableSuccessor());
                    System.out.println(toStringFingerTable());
                })
                .match(Stabelize.Request.class, msg -> {
                    StringBuilder sb = new StringBuilder();
                    sb.append(String.format("Stabilize: %4d - S: %4d  P: %4s  ", this.id, this.fingerTableSuccessor().id, (this.predecessor == null ? "x" : this.predecessorId)));

                    // TODO: Currently blocking, and thus is stuck sometimes > prevent RPC if call same node
                    ActorRef x = this.predecessor;
                    long xId = this.predecessorId;
                    if (getSelf() != this.fingerTable[0].chordRef) {
                        Timeout timeout = Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT));
                        Future<Object> fsFuture = Patterns.ask(this.fingerTable[0].chordRef, new Predecessor.Request(), Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT)));
                        Predecessor.Reply rply = (Predecessor.Reply) Await.result(fsFuture, timeout.duration());
                        x = rply.predecessor;
                        xId = rply.predecessorId;
                    }

                    sb.append(String.format(" -> x = ' is: %4d", xId));
                    // TODO: Refactor several ifs, which check x in (n , successor) including the "over-zero" case
                    if (x != null) {
                        // Node is the only one in the network (Should be removable!)
                        if (this.id == this.fingerTableSuccessor().id && xId != this.id) {
                            this.setSuccessor(x, xId);
                            sb.append(String.format("\t New S: %4d (Single Node Network) \n", this.fingerTableSuccessor().id));
                        } else if (isBetweenExeclusive(this.id, this.fingerTableSuccessor().id, xId)) {
                            this.setSuccessor(x, xId);
                            sb.append(String.format("\t New S: %4d \n", this.fingerTableSuccessor().id));
                        }
                    }
                    // Notify Successor that this node might be it's new predecessor
                    this.fingerTableSuccessor().chordRef.tell(new Notify.Request(getSelf(), this.id), getSelf());
                    System.out.println(sb.toString());
                })
                // .predecessor RPC
                .match(Predecessor.Request.class, msg -> {
                    getSender().tell(new Predecessor.Reply(this.predecessor, this.predecessorId), getSelf());
                })
                // Notify RPC:
                .match(Notify.Request.class, msg -> {
                    StringBuilder sb = new StringBuilder();
                    sb.append(String.format("Notify: %4d - S: %4d  P: %4s  \n", this.id, this.fingerTableSuccessor().id, (this.predecessor == null ? "x" : this.predecessorId)));
                    sb.append(String.format("\t N' is: %4d \n", msg.ndashId));
                    // TODO: Remove Dublicate Ifs (to conform to pseudocode)
                    if (this.predecessor == null) {
                        this.predecessor = msg.ndashActorRef;
                        this.predecessorId = msg.ndashId;
                        sb.append(String.format("\t New P: %4d (P was null) \n", this.predecessorId));
                    } else if (isBetweenExeclusive(this.predecessorId, this.id, msg.ndashId)) {
                        this.predecessor = msg.ndashActorRef;
                        this.predecessorId = msg.ndashId;
                        sb.append(String.format("\t New P: %4d (N' was between P and N \n)", this.predecessorId));
                    } else {
                        // Skip output if nothing changes
                        return;
                    }
                    System.out.println(sb.toString());
                })
                .match(FindSuccessor.Request.class, msg -> {
                    // +1 to do inclusive interval
                    // Single Node Edge Case: this.id == this.succId
                    if (this.id == this.fingerTableSuccessor().id) {
                        getContext().getSender().tell(new FindSuccessor.Reply(this.fingerTableSuccessor().chordRef, this.fingerTableSuccessor().id, msg.fingerTableIndex), getSelf());
                    } else if (isBetweenExeclusive(this.id, this.fingerTableSuccessor().id + 1, msg.id)) {
                        getContext().getSender().tell(new FindSuccessor.Reply(this.fingerTableSuccessor().chordRef, this.fingerTableSuccessor().id, msg.fingerTableIndex), getSelf());
                    } else {
                        // this.fingerTableSuccessor().chordRef.forward(msg, getContext());
                        ActorRef ndash = this.closest_preceding_node(msg.id);
                        ndash.forward(msg, getContext());
                    }
                })
                .match(FixFingers.Request.class, msg -> {
                    this.fix_fingers();
                })
                .match(UpdateFinger.Request.class, msg -> {

                    // Only Update If Change Necessary:
                    if (this.fingerTable[msg.fingerTableIndex] != null && (this.fingerTable[msg.fingerTableIndex].id == msg.fingerTableEntry.id
                            && this.fingerTable[msg.fingerTableIndex].chordRef.equals(msg.fingerTableEntry.chordRef))) {
                        return;
                    }

                    StringBuilder sb = new StringBuilder();
                    sb.append("Updating Finger Table Entry Number " + msg.fingerTableIndex + "\n");
                    sb.append("\t Previous Entry:\t " + this.fingerTable[msg.fingerTableIndex] + "\n");
                    sb.append("\t New Entry:\t " + msg.fingerTableEntry + "\n");
                    this.fingerTable[(int) msg.fingerTableIndex] = msg.fingerTableEntry;
                    System.out.println(sb.toString());

                })
                .match(Tcp.Bound.class, msg -> {
                    // This will be called, when the SystemActor bound MemCache interface for the particular node.
                    manager.tell(msg, getSelf());
                    System.out.printf("MemCache Interface for node %s listening to %s \n", getSelf().toString(), msg.localAddress().toString());
                })
                .match(CommandFailed.class, msg -> {
                    System.out.println("Command failed");
                    if (msg.cmd() instanceof Tcp.Bind) {
                        int triedPort = ((Tcp.Bind) msg.cmd()).localAddress().getPort();
                        if (triedPort <= Node.MEMCACHE_MAX_PORT) {
                            System.out.println("Port Binding Failed; Retrying...");
                            createMemCacheTCPSocket(triedPort + 1);
                        } else {
                            System.out.println("Port Binding Failed; Ports for Memcache Interface exhausted");
                            System.out.println("Shutting down...");
                            getContext().stop(getSelf());
                        }
                    }
                })
                .match(Connected.class, conn -> {
                    System.out.println("MemCache Client connected");
                    manager.tell(conn, getSelf());
                    ActorRef memcacheHandler = getContext().actorOf(Props.create(MemcachedActor.class, storageActorRef = this.storageActorRef));
                    getSender().tell(TcpMessage.register(memcacheHandler), getSelf());
                })
                .match(KeyValue.Put.class, putValueMessage -> {
                    String key = putValueMessage.key;
                    Serializable value = putValueMessage.value;
                    log.info("key, value: " + key + " " + value);
                    this.storageActorRef.forward(putValueMessage, getContext());
                })
                .match(KeyValue.Get.class, getValueMessage -> {
                    this.storageActorRef.forward(getValueMessage, getContext());
                })
                .build();
    }

    private FingerTableEntry fingerTableSuccessor() {
        return this.fingerTable[0];
    }

    private void setSuccessor(ActorRef af, long id) {
        FingerTableEntry fte = new FingerTableEntry();
        fte.chordRef = af;
        fte.id = id;
        this.fingerTable[0] = fte;
    }

    private long getNodeId() {
        long envVal;
        HashUtil hashUtil = new HashUtil();
        if (System.getenv("NODE_ID") == null) {
            String hostName = config.getString("akka.remote.artery.canonical.hostname");
            String port = config.getString("akka.remote.artery.canonical.port");
            // FIXME Should be IP
            envVal = Math.floorMod(hashUtil.hash(hostName + ":" + port), AMOUNT_OF_KEYS);
        } else {
            envVal = Long.parseLong(System.getenv("NODE_ID"));
        }

        return envVal;
    }

    private String getNodeType() {
        String nodeType = config.getString("myapp.nodeType");

        if (System.getenv("CHORD_NODE_TYPE") != null) {
            nodeType = System.getenv("CHORD_NODE_TYPE");
        }
        return nodeType;
    }

    /**
     * Returns the akka address for the central node.
     * This is either fed by config variables, or by enviromental variables.
     *
     * @return
     */
    private String getCentralNodeAddress() {
        String centralEntityAddress = config.getString("myapp.centralEntityAddress");
        String centralEntityAddressPort = config.getString("myapp.centralEntityPort");
        if (System.getenv("CHORD_CENTRAL_NODE") != null) {
            centralEntityAddress = System.getenv("CHORD_CENTRAL_NODE");
            centralEntityAddressPort = System.getenv("CHORD_CENTRAL_NODE_PORT");
            try {
                InetAddress address = InetAddress.getByName(centralEntityAddress);
                centralEntityAddress = address.getHostAddress();
                System.out.println(address.getHostAddress());
            } catch (Exception e) {
                // TODO: need to handle
            }
        }

        return "akka://ChordNetwork@" + centralEntityAddress + ":" + centralEntityAddressPort + "/user/ChordActor";
    }

    private ActorRef closest_preceding_node(long id) {
        for (int i = Node.m - 1; i >= 0; i--) {
            if (this.fingerTable[i] == null)
                continue;
            if (isBetweenExeclusive(this.id, id, this.fingerTable[i].id))
                return this.fingerTable[i].chordRef;
        }
        return getSelf();
    }

    private void fix_fingers() {
        fix_fingers_next++;

        if (fix_fingers_next > Node.m) {
            fix_fingers_next = 1;
        }
        long idx = (long) Math.pow(2, fix_fingers_next - 1);
        long lookup_id = (long) this.id + idx % (long) Math.pow(2, Node.m);

        // Get The Successor For This Id
        Future<Object> fsFuture = Patterns.ask(this.fingerTableSuccessor().chordRef, new FindSuccessor.Request(lookup_id, fix_fingers_next - 1), Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT)));
        fsFuture.onComplete(
                new OnComplete<Object>() {
                    public void onComplete(Throwable failure, Object result) {
                        if (failure != null) {
                            // We got a failure, handle it here
                            System.out.println("Something went wrong");
                            System.out.println(failure);
                        } else {
                            FindSuccessor.Reply fsrpl = (FindSuccessor.Reply) result;
                            FingerTableEntry fte = new FingerTableEntry();
                            fte.chordRef = fsrpl.succesor;
                            fte.id = fsrpl.id;
                            UpdateFinger.Request ufReq = new UpdateFinger.Request(fsrpl.fingerTableIndex, fte);
                            getSelf().tell(ufReq, getSelf());
                        }
                    }
                }, getContext().system().dispatcher());
    }

    private String toStringFingerTable() {
        StringBuilder sb = new StringBuilder();
        sb.append("---Finger Table---\n");
        for (int i = 0; i < this.fingerTable.length; i++) {
            if (this.fingerTable[i] == null) {
                sb.append(i + " -- empty \n");
            } else {
                sb.append(i + " -- " + this.fingerTable[i].id + " - " + this.fingerTable[i].chordRef.toString() + "\n");
            }

        }
        return sb.toString();
    }

    private void createMemCacheTCPSocket() {
        createMemCacheTCPSocket(Node.MEMCACHE_MIN_PORT);
        // TODO: Environment Var Control?
    }

    private void createMemCacheTCPSocket(int port) {

        final ActorRef tcp = Tcp.get(getContext().getSystem()).manager();
        // Get possible hostname:
        String hostname = "localhost";

        if (System.getenv("HOSTNAME") != null) {
            hostname = System.getenv("HOSTNAME");
        }

        // Calculate a unique port based on the id, if the port is already taken:
        if (isPortInUse(hostname, port)) {
            // TODO: Nicer heuristic to find a good suitable port
            port = port + (int) this.id;
        }

        InetSocketAddress tcp_socked = new InetSocketAddress(hostname, port);
        Tcp.Command tcpmsg = TcpMessage.bind(getSelf(), tcp_socked, 100);
        tcp.tell(tcpmsg, getSelf());
    }

    private boolean isPortInUse(String host, int port) {
        // Assume no connection is possible.
        boolean result = false;

        try {
            (new Socket(host, port)).close();
            result = true;
        } catch (SocketException e) {
            // Could not connect.
        } catch (Exception e) {
            System.out.println();
        }

        return result;
    }

}
