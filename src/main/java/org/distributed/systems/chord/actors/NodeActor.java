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
import org.distributed.systems.chord.models.ChordNode;
import org.distributed.systems.chord.models.Pair;
import org.distributed.systems.chord.service.FingerTableService;
import org.distributed.systems.chord.util.CompareUtil;
import org.distributed.systems.chord.util.Util;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class NodeActor extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private FingerTableService fingerTableService;

    private static final int MEMCACHE_MIN_PORT = 11211;
    private static final int MEMCACHE_MAX_PORT = 12235;
    private static final int STABILIZE_SCHEDULE_TIME = 5000;
    private static final int FIX_FINGER_SCHEDULE_TIME = 1000;
    private final ActorRef manager;

    private ActorRef storageActorRef;
    private Config config = getContext().getSystem().settings().config();

    private String type;
    private long nodeId;
    private int fix_fingers_next = 0;

    public NodeActor() {
        this.manager = Tcp.get(getContext().getSystem()).manager();
        this.storageActorRef = getContext().actorOf(Props.create(StorageActor.class));
        this.type = Util.getNodeType(config);
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        log.info("Starting up...     ref: " + getSelf());
        this.nodeId = Util.getNodeId(config);

        // Init the FingerTable
        fingerTableService = new FingerTableService(this.nodeId);

        createMemCacheTCPSocket();

        if (this.type.equals("central")) {
            fingerTableService.setSuccessor(new ChordNode(this.nodeId, getSelf()));
            System.out.println("Bootstrapped Central NodeActor");
            getSelf().tell(new Stabilize.Request(), getSelf());
        } else {
            final String centralNodeAddress = Util.getCentralNodeAddress(config);
            Timeout timeout = Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT));
            Future<ActorRef> centralNodeFuture = getContext().actorSelection(centralNodeAddress).resolveOne(timeout);
            ActorRef centralNode = Await.result(centralNodeFuture, timeout.duration());
            JoinMessage.JoinRequest joinRequestMessage = new JoinMessage.JoinRequest(centralNode, this.nodeId);
            System.out.println("Bootstrapped Regular NodeActor");
            getSelf().tell(joinRequestMessage, getSelf());
        }
        // This will schedule to send the Stabilize-message
        // to the stabilizeActor after 0ms repeating every 5000ms
        ActorRef stabilizeActor = getContext().actorOf(Props.create(StabilizeActor.class, getSelf()));
        getContext().getSystem().scheduler().scheduleWithFixedDelay(Duration.ZERO, Duration.ofMillis(STABILIZE_SCHEDULE_TIME), stabilizeActor, "Stabilize", getContext().system().dispatcher(), ActorRef.noSender());

        // This will schedule to send the FixFinger-message
        // to the fixFingerActor after 0ms repeating every 1000ms
        ActorRef fixFingerActor = getContext().actorOf(Props.create(FixFingerActor.class, getSelf()));
        getContext().getSystem().scheduler().scheduleWithFixedDelay(Duration.ZERO, Duration.ofMillis(FIX_FINGER_SCHEDULE_TIME), fixFingerActor, "FixFinger", getContext().system().dispatcher(), ActorRef.noSender());
    }

    private void getValueForKey(long hashKey) {
        if (shouldKeyBeOnThisNodeOtherwiseForward(hashKey, new KeyValue.Get(hashKey))) {
            getValueFromStorageActor(hashKey);
        }
    }

    private void getValueFromStorageActor(long hashKey) {
        KeyValue.Get getRequest = new KeyValue.Get(hashKey);
        Timeout timeout = Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT));
        Future<Object> valueResponse = Patterns.ask(this.storageActorRef, getRequest, timeout);
        try {
            Pair<String, Serializable> value = ((KeyValue.GetReply) Await.result(valueResponse, timeout.duration())).value;
            getSender().tell(new KeyValue.GetReply(hashKey, value), getSelf());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //FIXME: Merge Conflict
    private void putValueForKey(long hashKey, Pair<String, Serializable> value) {
        if (shouldKeyBeOnThisNodeOtherwiseForward(hashKey, new KeyValue.Put(hashKey, value))) {
            putValueInStore(hashKey, value);
            getSender().tell(new KeyValue.PutReply(hashKey, value), getSelf());
        }
    }
//    private void putValueForKey(long hashKey, String originalKey, Serializable value) {
//        putValueForKey(new KeyValue.Put(originalKey, hashKey, value));
//    }

//    private void putValueForKey(KeyValue.Put msg) {
//        if (shouldKeyBeOnThisNodeOtherwiseForward(msg.hashKey, msg)) {
//            putValueInStore(msg);
//            getSender().tell(new KeyValue.PutReply(msg.originalKey, msg.hashKey, msg.value), getSelf());
//        }
//    }

    private void deleteKey(KeyValue.Delete msg) {
        if (shouldKeyBeOnThisNodeOtherwiseForward(msg.hashKey, msg)) {
            // TODO: What if not found, memcached actually likes "not found" things
            deleteKeyInStore(msg);
            getSender().tell(new KeyValue.DeleteReply(), getSelf());
        }
    }

    private void deleteKeyInStore(KeyValue.Delete msg) { storageActorRef.tell(msg, getSelf()); }

    private void putValueInStore(KeyValue.Put msg) {
        storageActorRef.tell(msg, getSelf());
    }

    //FIXME: Merge Conflict
    private void putValueInStore(long hashKey, Pair<String, Serializable> value) {
        storageActorRef.tell(new KeyValue.Put(hashKey, value), getSelf());
    }
//    private void putValueInStore(long hashKey, String originalKey, Serializable value) {
//        putValueInStore(new KeyValue.Put(originalKey, hashKey, value));
//    }

    private boolean shouldKeyBeOnThisNodeOtherwiseForward(long key, Command commandMessage) {
        if (nodeId == fingerTableService.getSuccessor().id) { // I'm the only node in the network
            return true;
        }

        // Between my predecessor and my node id
        if (CompareUtil.isBetweenExclusive(fingerTableService.getPredecessor().id, nodeId + 1, key)) {
            return true;
        } else if (CompareUtil.isBetweenExclusive(nodeId, fingerTableService.getSuccessor().id + 1, key)) {
            fingerTableService.getSuccessor().chordRef.forward(commandMessage, getContext());
            return false;
        } else {
            closest_preceding_node(key).forward(commandMessage, getContext());
            return false;
        }
    }

    @Override
    public Receive createReceive() {
        log.info("Received a message");

        return receiveBuilder()
                .match(JoinMessage.JoinRequest.class, msg -> {
                    System.out.println("NodeActor " + this.nodeId + " wants to join");

                    Timeout timeout = Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT));
                    Future<Object> centralNodeFuture = Patterns.ask(msg.requestor, new FindSuccessor.Request(this.nodeId, 0), timeout);
                    FindSuccessor.Reply rply = (FindSuccessor.Reply) Await.result(centralNodeFuture, timeout.duration());

                    fingerTableService.setSuccessor(new ChordNode(rply.id, rply.succesor));
                    System.out.println("NodeActor " + this.nodeId + "joined! ");
                    System.out.println("Successor: " + this.fingerTableService.getSuccessor());
                    fingerTableService.printFingerTable(true);

                })
                .match(GetActorRef.Request.class, requestMessage ->{
                    getSender().tell(new GetActorRef.Reply(this.storageActorRef), getSelf());
                })
                .match(Stabilize.Request.class, msg -> {

                    // TODO: Currently blocking, and thus is stuck sometimes > prevent RPC if call same node
                    ChordNode x = fingerTableService.getPredecessor();
                    if (getSelf() != fingerTableService.getSuccessor().chordRef) {
                        Timeout timeout = Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT));
                        Future<Object> fsFuture = Patterns.ask(fingerTableService.getSuccessor().chordRef, new Predecessor.Request(), timeout);
                        Predecessor.Reply rply = (Predecessor.Reply) Await.result(fsFuture, timeout.duration());

                        x = rply.predecessor;
                    }

                    // TODO: Refactor several ifs, which check x in (n , successor) including the "over-zero" case
                    if (x.chordRef != null) {
                        // NodeActor is the only one in the network (Should be removable!)
                        if (this.nodeId == this.fingerTableService.getSuccessor().id && x.id != this.nodeId) {
                            fingerTableService.setSuccessor(x);
                        } else if (CompareUtil.isBetweenExclusive(this.nodeId, this.fingerTableService.getSuccessor().id, x.id)) {
                            fingerTableService.setSuccessor(x);
                        }
                    }
                    // Notify Successor that this node might be it's new predecessor
                    this.fingerTableService.getSuccessor().chordRef.tell(new Notify.Request(new ChordNode(this.nodeId, getSelf())), getSelf());
                    // System.out.println(fingerTableService.toString());
                })
                // .predecessor RPC
                .match(Predecessor.Request.class, msg -> getSender().tell(new Predecessor.Reply(fingerTableService.getPredecessor()), getSelf()))
                // Notify RPC:
                .match(Notify.Request.class, msg -> {
                    // TODO: Remove Dublicate Ifs (to conform to pseudocode)
                    if (fingerTableService.getPredecessor().chordRef == null) {
                        fingerTableService.setPredecessor(msg.nPrime);
                        transferKeysOnJoin();
                    } else if (CompareUtil.isBetweenExclusive(fingerTableService.getPredecessor().id, this.nodeId, msg.nPrime.id)) {
                        fingerTableService.setPredecessor(msg.nPrime);
                    } else {
                        // Skip output if nothing changes
                        return;
                    }
                    // System.out.println(fingerTableService.toString());
                })
                .match(FindSuccessor.Request.class, msg -> {
                    // +1 to do inclusive interval
                    // Single NodeActor Edge Case: this.nodeId == this.succId
                    if (this.nodeId == this.fingerTableService.getSuccessor().id) {
                        getContext().getSender().tell(new FindSuccessor.Reply(this.fingerTableService.getSuccessor().chordRef, this.fingerTableService.getSuccessor().id, msg.fingerTableIndex), getSelf());
                    } else if (CompareUtil.isBetweenExclusive(this.nodeId, this.fingerTableService.getSuccessor().id + 1, msg.id)) {
                        getContext().getSender().tell(new FindSuccessor.Reply(this.fingerTableService.getSuccessor().chordRef, this.fingerTableService.getSuccessor().id, msg.fingerTableIndex), getSelf());
                    } else {
                        ActorRef ndash = this.closest_preceding_node(msg.id);
                        ndash.forward(msg, getContext());
                    }
                })
                .match(FixFingers.Request.class, msg -> this.fix_fingers())
                .match(UpdateFinger.Request.class, msg -> {

                    // Only Update If Change Necessary:
                    ChordNode finger = fingerTableService.getEntryForIndex(msg.fingerTableIndex);
                    if (finger != null && (finger.id.equals(msg.chordNode.id) && finger.chordRef.equals(msg.chordNode.chordRef))) {
                        return;
                    }

                    fingerTableService.setFingerEntryForIndex(msg.fingerTableIndex, msg.chordNode);
                    fingerTableService.printFingerTable(false);

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
                        if (triedPort <= NodeActor.MEMCACHE_MAX_PORT) {
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
                    manager.tell(conn, getSelf());
                    ActorRef memcacheHandler = getContext().actorOf(Props.create(MemcachedActor.class, self()));
                    getSender().tell(TcpMessage.register(memcacheHandler), getSelf());
                })
                .match(KeyValue.Put.class, putValueMessage -> {

                    long hashKey = putValueMessage.hashKey;
                    Pair<String, Serializable> value = putValueMessage.value;
                    putValueForKey(hashKey, value);
                    //FIXME: Merge Conflict
                    // We need to pass the original message, to ensure that memcache actor gets a reply
//                    putValueForKey(putValueMessage);
                })
                .match(KeyValue.Delete.class, deleteMessage -> {
                    deleteKey(deleteMessage);
                })
                .match(KeyValue.Get.class, getValueMessage -> getValueForKey(getValueMessage.hashKey))
                .build();
    }

    private ActorRef closest_preceding_node(long id) {
        for (int i = ChordStart.M - 1; i >= 0; i--) {
            if (fingerTableService.getEntryForIndex(i) == null)
                continue;
            if (CompareUtil.isBetweenExclusive(this.nodeId, id, fingerTableService.getEntryForIndex(i).id))
                return fingerTableService.getEntryForIndex(i).chordRef;
        }
        return getSelf();
    }

    private void fix_fingers() {
        fix_fingers_next++;

        if (fix_fingers_next > ChordStart.M) {
            fix_fingers_next = 1;
        }
        long idx = (long) Math.pow(2, fix_fingers_next - 1);
        long lookup_id = (this.nodeId + idx) % ChordStart.AMOUNT_OF_KEYS;

        // Get The Successor For This Id
        Future<Object> fsFuture = Patterns.ask(this.fingerTableService.getSuccessor().chordRef, new FindSuccessor.Request(lookup_id, fix_fingers_next - 1), Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT)));
        fsFuture.onComplete(
                new OnComplete<Object>() {
                    public void onComplete(Throwable failure, Object result) {
                        if (failure != null) {
                            // We got a failure, handle it here
                            System.out.println("Something went wrong");
                            failure.printStackTrace();
                        } else {
                            FindSuccessor.Reply fsrpl = (FindSuccessor.Reply) result;
                            ChordNode fte = new ChordNode(fsrpl.id, fsrpl.succesor);
                            UpdateFinger.Request ufReq = new UpdateFinger.Request(fsrpl.fingerTableIndex, fte);
                            getSelf().tell(ufReq, getSelf());
                        }
                    }
                }, getContext().system().dispatcher());
    }


    private void createMemCacheTCPSocket() {
        createMemCacheTCPSocket(NodeActor.MEMCACHE_MIN_PORT);
        // TODO: Environment Var Control?
    }

    private void createMemCacheTCPSocket(int port) {

        final ActorRef tcp = Tcp.get(getContext().getSystem()).manager();
        // Get possible hostname:
        String hostname = "localhost";

        if (System.getenv("HOSTNAME") != null) {
            hostname = System.getenv("HOSTNAME");
        }

        // Calculate a unique port based on the nodeId, if the port is already taken:
        if (isPortInUse(hostname, port)) {
            // TODO: Nicer heuristic to find a good suitable port
            port = port + 1;
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

    private void transferKeysOnJoin(){
        // calculate key range by looking at predecessor value
        List<Long> keyRange = LongStream.range(this.fingerTableService.getPredecessor().id + 1, this.nodeId)
                .boxed()
                .collect(Collectors.toList());

        // tell my storageActor to ask my successor for transfer keys.
        ActorRef successor = fingerTableService.getSuccessor().chordRef;
        if(getSelf() != successor){
            System.out.println("Transferring keys...");
            this.storageActorRef.tell(new KeyTransfer.Request(successor, keyRange), getSelf());
        }
    }

}
