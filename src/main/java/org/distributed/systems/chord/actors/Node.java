package org.distributed.systems.chord.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
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
import org.distributed.systems.chord.messaging.Command;
import org.distributed.systems.chord.messaging.JoinMessage;
import org.distributed.systems.chord.messaging.KeyValue;
import org.distributed.systems.chord.util.CompareUtil;
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

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    public static final int m = 3; // Number of bits in key id's
    public static final long AMOUNT_OF_KEYS = Math.round(Math.pow(2, m));
    static final int MEMCACHE_MIN_PORT = 11211;
    static final int MEMCACHE_MAX_PORT = 12235;
    final ActorRef manager;

    private ActorRef storageActorRef;
    private Config config = getContext().getSystem().settings().config();

    private ActorRef predecessor = null;
    private long predecessorId;
    private ActorRef sucessor = null;
    private long sucessorId;
    private ActorRef centralNode = null;
    private String type = "";
    private long id;

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

        // Generating Id:
//        Random randomGenerator = new Random();
//        long randomInt = randomGenerator.nextInt(ChordStart.M);
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

        this.id = envVal;
        System.out.println("Node Id " + this.id);

        if (this.type.equals("central")) {
            this.predecessor = getSelf();
            this.predecessorId = this.id;
            this.sucessor = getSelf();
            this.sucessorId = this.id;
            System.out.println("Started As Central Node");
        } else {
            System.out.println("Started As Regular Node");
            final String centralNodeAddress = getCentralNodeAddress();
            Timeout timeout = Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT));
            System.out.println("Looking For Central Node");
            Future<ActorRef> centralNodeFuture = getContext().actorSelection(centralNodeAddress).resolveOne(timeout);
            this.centralNode = (ActorRef) Await.result(centralNodeFuture, timeout.duration());
            System.out.println("Found Central Node");
        }

        if (this.centralNode != null) {
            // Request a Join
            JoinMessage.JoinRequest joinRequestMessage = new JoinMessage.JoinRequest(getSelf(), this.id);
            this.centralNode.tell(joinRequestMessage, getSelf());
            // TODO: Retry if this fails (as central node does not respond)
        }
        this.createMemCacheTCPSocket();
    }

    private void getValueForKey(long key) {
        if (shouldKeyBeOnThisNodeOtherwiseForward(key, new KeyValue.Get(key))) {
            getValueFromStorageActor(key);
        }
    }

    private void getValueFromStorageActor(long key) {
        KeyValue.Get getRequest = new KeyValue.Get(key);
        Timeout timeout = Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT));
        Future<Object> valueResponse = Patterns.ask(this.storageActorRef, getRequest, timeout);
        try {
            Serializable value = ((KeyValue.GetReply) Await.result(valueResponse, timeout.duration())).value;
            getSender().tell(new KeyValue.GetReply(key, value), getSelf());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void putValueForKey(long key, Serializable value) {
        if (shouldKeyBeOnThisNodeOtherwiseForward(key, new KeyValue.Put(key, value))) {
            putValueInStore(key, value);
            getSender().tell(new KeyValue.PutReply(key, value), getSelf());
        }
    }

    private void putValueInStore(long key, Serializable value) {
        storageActorRef.tell(new KeyValue.Put(key, value), getSelf());
    }

    private boolean shouldKeyBeOnThisNodeOtherwiseForward(long key, Command commandMessage) {
        // Between my predecessor and my node id
        if (CompareUtil.between(this.predecessorId, false, this.id, true, key)) {
            return true;
        } else {
            this.sucessor.forward(commandMessage, getContext());
            return false;
        }
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

    @Override
    public Receive createReceive() {
        log.info("Received a message");

        return receiveBuilder()
                .match(JoinMessage.JoinRequest.class, msg -> {
                    System.out.println("A node asked to join");
                    handleJoinRequest(msg);
                })
                .match(JoinMessage.JoinReply.class, msg -> {
                    if (msg.accepted) {

                        this.predecessor = msg.predecessor;
                        this.predecessorId = msg.predecessorId;

                        this.sucessor = msg.sucessor;
                        this.sucessorId = msg.sucessorId;

                        System.out.println("Regular Node " + this.id + " joined by receiving a JoinReply");
                        System.out.println("My Successor:" + this.sucessor.toString() + " with id:" + this.sucessorId);
                        System.out.println("My Predecessor:" + this.predecessor.toString() + " with id:" + this.predecessorId);
                    } else {
                        System.out.println("Join was not accepted for Node" + this.id);
                        getContext().stop(getSelf());
                    }
                })
                .match(JoinMessage.JoinConfirmationRequest.class, msg -> {
                    // Either Successor or Predecessor wants insert a new node.
                    System.out.println("Node" + this.id + " is requested to confirm a JoinRequest");

                    // Determine if a successor or predecessor
                    if (msg.newPredecessor == null && msg.newSucessor != null) {
                        // it's a successor
                        this.sucessorId = msg.newSucessorKey;
                        this.sucessor = msg.newSucessor;
                        getContext().getSender().tell(new JoinMessage.JoinConfirmationReply(true), ActorRef.noSender());
                        System.out.println("Node " + this.id + " confirmed the JoinRequest");
                        System.out.println("Node's " + this.id + "current predecessor: " + this.predecessorId);
                        System.out.println("Node's " + this.id + " new successor: " + msg.newSucessorKey);
                    } else if (msg.newPredecessor != null && msg.newSucessor == null) {
                        // it's a predecessor
                        this.predecessorId = msg.newPredecessorKey;
                        this.predecessor = msg.newPredecessor;
                        JoinMessage.JoinConfirmationReply confirmReplyMsg = new JoinMessage.JoinConfirmationReply(true);
                        getContext().getSender().tell(confirmReplyMsg, ActorRef.noSender());
                        System.out.println("Node " + this.id + " confirmed the JoinRequest");
                        System.out.println("Node's " + this.id + "current successor: " + this.sucessorId);
                        System.out.println("Node's " + this.id + " new predecessor: " + msg.newPredecessorKey);
                    } else {
                        // Some illegal state, decline this request
                        System.out.println("Node " + this.id + " DECLINED the JoinRequest");
                        getContext().getSender().tell(new JoinMessage.JoinConfirmationReply(false), ActorRef.noSender());
                    }
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
                    ActorRef memcacheHandler = getContext().actorOf(Props.create(MemcachedActor.class, this.storageActorRef, getSelf()));
                    getSender().tell(TcpMessage.register(memcacheHandler), getSelf());
                })
                .match(KeyValue.Put.class, putValueMessage -> {
                    long key = putValueMessage.key;
                    Serializable value = putValueMessage.value;
                    putValueForKey(key, value);
                })
                .match(KeyValue.Get.class, getValueMessage -> {
                    getValueForKey(getValueMessage.key);
                })
                .build();
    }

    /**
     * This handles a join request from a node:
     * First the Error handling for dangling node + same key is done.
     * Then based on the requester's key it is determined if
     * the node can be inserted from this node, or the msg needs
     * to be forwarded to another node in the network
     * @param msg
     * @throws Exception
     */
    private void handleJoinRequest(JoinMessage.JoinRequest msg) throws Exception {
        // Handle Error Case - Regular Node, that is not part of a network
        if (this.predecessor == null && this.sucessor == null && this.type.equals("regular")) {
            msg.requestor.tell(new JoinMessage.JoinReply(null, null, false), getSelf());
            System.out.println("I declined the JOIN, I am a regular Node being part of no network");
            return;
        }

        // Handle Error Case - Asked NodeId is already present.
        if (msg.requestorKey == this.id) {
            msg.requestor.tell(new JoinMessage.JoinReply(null, null, false), getSelf());
            System.out.println("I declined the JOIN, node that request join has same key!");
            return;
        }


        if (this.predecessor == getSelf() && this.sucessor == getSelf()) {
            // Initial Situation: central node is the only node in the ring -> insert the requesting node
            this.sucessor = msg.requestor;
            this.sucessorId = msg.requestorKey;
            this.predecessor = msg.requestor;
            this.predecessorId = msg.requestorKey;
            msg.requestor.tell(new JoinMessage.JoinReply(getSelf(), getSelf(), true, this.id, this.id), getSelf());
        } else {
            // Handle Cases for Chord Network with more than 1 node (4 general cases)
            if (msg.requestorKey < this.id) {
                if (msg.requestorKey < this.predecessorId) {
                    // 1. Smaller than predecessor -> Either forward or handle edge case
                    if (this.id < this.predecessorId) {
                        // Edge Case: Join Request passing 0 in the ring ->
                        // Current node's predecessor is bigger -> Requester can be inserted as new predecessor
                        handleJoinInsertAsPredecessor(msg);
                    } else {
                        System.out.println("Predecessor needs to handle the Join Request");
                        this.predecessor.forward(msg, getContext());
                    }
                } else {
                    handleJoinInsertAsPredecessor(msg);
                }

            } else if (msg.requestorKey > this.id) {

                if (msg.requestorKey > this.sucessorId) {
                    // 4. Greater than successor -> Either forward or handle edge case
                    if (this.sucessorId < this.id) {
                        // Edge Case: Join Request passing 0 in the ring ->
                        // Current node's successor is smaller -> Requester can be inserted as new successor
                        handleJoinInsertAsSuccessor(msg);
                    } else {
                        System.out.println("Successor needs to handle the Join Request");
                        this.sucessor.forward(msg, getContext());
                    }
                } else {
                    handleJoinInsertAsSuccessor(msg);
                }
            } else {
                // Else: Keys are equal: Reject join
                JoinMessage.JoinReply joinReplyMessage = new JoinMessage.JoinReply(null, null, false);
                msg.requestor.tell(joinReplyMessage, getSelf());
                System.out.println("I declined the JOIN, node that request join has same key of a node in the network!");
                return;
            }
        }
        System.out.println("I accepted a Join Request from " + msg.requestorKey);
        System.out.println("New Successor:" + this.sucessor.toString() + " with id:" + this.sucessorId);
        System.out.println("New Predecessor:" + this.predecessor.toString() + " with id:" + this.predecessorId);
    }


    /**
     * Handles the case, when a requestor can be added as the sucessor of this node.
     * This might be the case when:
     * It's in between the node's id and the successor
     * or when it's the requestor's key is the biggest id in the chord network.
     * @param msg
     * @throws Exception
     */
    private void handleJoinInsertAsSuccessor(JoinMessage.JoinRequest msg) throws Exception {
        JoinMessage.JoinConfirmationRequest joinConfirmationRequestMessage = new JoinMessage.JoinConfirmationRequest(msg.requestor, msg.requestorKey, null, 0);
        Timeout timeout = Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT));
        Future<Object> confirmationReqFuture = Patterns.ask(this.sucessor, joinConfirmationRequestMessage, timeout);
        JoinMessage.JoinConfirmationReply result = (JoinMessage.JoinConfirmationReply) Await.result(confirmationReqFuture, timeout.duration());
        //TODO: Handle timeout!
        if (result.accepted) {
            System.out.println("I return a successor" + this.sucessorId);
            System.out.println("I am " + this.id);
            JoinMessage.JoinReply joinReplyMessage = new JoinMessage.JoinReply(getSelf(), this.sucessor, true, this.id, this.sucessorId);
            msg.requestor.tell(joinReplyMessage, getSelf());
            this.sucessor = msg.requestor;
            this.sucessorId = msg.requestorKey;
            System.out.println("I confirmed the final join!");
            System.out.println("My new successor: " + msg.requestorKey);
            return;
        } else {
            JoinMessage.JoinReply joinReplyMessage = new JoinMessage.JoinReply(null, null, false);
            msg.requestor.tell(joinReplyMessage, getSelf());
            System.out.println("I declined the JOIN, predecessor rejected join!");
            return;
        }
    }

    /**
     * Handles the case, when a requester can be added as the predecessor of this node.
     * This might be the case when:
     * It's in between the node's id and the predecessor's id
     * or when it's the requester's key is the smallest id in the chord network.
     * @param msg
     * @throws Exception
     */
    private void handleJoinInsertAsPredecessor(JoinMessage.JoinRequest msg) throws Exception {
        JoinMessage.JoinConfirmationRequest joinConfirmationRequestMessage = new JoinMessage.JoinConfirmationRequest(null, 0, msg.requestor, msg.requestorKey);
        Timeout timeout = Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT));
        Future<Object> confirmationReqFuture = Patterns.ask(this.predecessor, joinConfirmationRequestMessage, timeout);
        JoinMessage.JoinConfirmationReply result = (JoinMessage.JoinConfirmationReply) Await.result(confirmationReqFuture, timeout.duration());
        //TODO: Handle timeout!
        if (result.accepted) {
            JoinMessage.JoinReply joinReplyMessage = new JoinMessage.JoinReply(this.predecessor, getSelf(), true, this.predecessorId, this.id);
            msg.requestor.tell(joinReplyMessage, getSelf());
            this.predecessor = msg.requestor;
            this.predecessorId = msg.requestorKey;
            System.out.println("I confirmed the final join!");
            System.out.println("My new predecessor: " + msg.requestorKey);
            return;
        } else {
            JoinMessage.JoinReply joinReplyMessage = new JoinMessage.JoinReply(null, null, false);
            msg.requestor.tell(joinReplyMessage, getSelf());
            System.out.println("I declined the JOIN, predecessor rejected join!");
            return;
        }
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
        }
        catch(SocketException e) {
            // Could not connect.
        }
        catch (Exception e) {
            System.out.println();
        }

        return result;
    }

}
