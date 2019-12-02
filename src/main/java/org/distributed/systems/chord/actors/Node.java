package org.distributed.systems.chord.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.io.Tcp;
import akka.io.Tcp.CommandFailed;
import akka.io.Tcp.Connected;
import akka.io.TcpMessage;
import com.typesafe.config.Config;
import org.distributed.systems.ChordStart;
import org.distributed.systems.chord.messaging.*;
import org.distributed.systems.chord.model.ChordNode;
import org.distributed.systems.chord.model.finger.Finger;
import org.distributed.systems.chord.model.finger.FingerInterval;
import org.distributed.systems.chord.service.FingerTableService;
import org.distributed.systems.chord.util.CompareUtil;
import org.distributed.systems.chord.util.Util;
import org.distributed.systems.chord.util.impl.HashUtil;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Random;

public class Node extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    static final int MEMCACHE_MIN_PORT = 11211;
    static final int MEMCACHE_MAX_PORT = 12235;
    final ActorRef manager;

    private int generateFingerCount = 1;
    private static ChordNode node;
    private FingerTableService fingerTableService;
    private ActorRef storageActorRef;
    private Config config = getContext().getSystem().settings().config();
    private boolean memCacheEnabled = false;

    public Node() {
        fingerTableService = new FingerTableService();
        this.manager = Tcp.get(getContext().getSystem()).manager();
        this.storageActorRef = getContext().actorOf(Props.create(StorageActor.class));

    }

    public static Props props(ActorRef manager) {
        return Props.create(Node.class, manager);
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        log.info("Starting up...     ref: " + getSelf());
        long envVal;
        if (System.getenv("node.id") == null) {
            envVal = new HashUtil().hash(getSelf().path().toSerializationFormat()); // FIXME Should be IP
        } else {
            envVal = Long.parseLong(System.getenv("node.id"));
        }
        final long NODE_ID = envVal;
        System.out.println("Starting up with node_id: " + NODE_ID);

        node = new ChordNode(NODE_ID, Util.getIp(config), Util.getPort(config));
        if (memCacheEnabled) {
            this.createMemCacheTCPSocket();
        }
        joinNetwork();
    }

    private void createMemCacheTCPSocket() {
        createMemCacheTCPSocket(Node.MEMCACHE_MIN_PORT);
        // TODO: Environment Var Control?
    }

    private void createMemCacheTCPSocket(int port) {

        final ActorRef tcp = Tcp.get(getContext().getSystem()).manager();
        // TODO: We need to expose this port to the outer world
        InetSocketAddress tcp_socked = new InetSocketAddress("localhost", port);
        Tcp.Command tcpmsg = TcpMessage.bind(getSelf(), tcp_socked, 100);
        tcp.tell(tcpmsg, getSelf());
    }

    @Override
    public Receive createReceive() {
        log.info("Received a message");

        return receiveBuilder()
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
                .match(FindSuccessor.class, findSuccessor -> {
                    log.info("FindSuccessor");
                    // When you get a message from yourself
                    if (getSender().equals(getSelf())) {
                        // I will set my successor
                        fingerTableService.getFingers().get(generateFingerCount).setSucc(fingerTableService.getSuccessor());

                        // Check if we need more finger entries
                        if (isFingerTableNotComplete()) {
                            loopAndFixFingers();
                        } else {
                            printFingerTable();
                            updateOthers();
                        }
                        return;
                    }

                    // Find predecessor --> in reply send FindSuccessorReply
                    tellFindPredecessor(findSuccessor.getId(), FindPredecessorReply.UNSET, getSender());
                })
                .match(FindSuccessorReply.class, findSuccessorReply -> {
                    // Reply on the DirectGetSuccessor message (that is called after the findPredecessor)
//                    log.info("FindSuccessorReply");

                    Util.getActorRef(getContext(), findSuccessorReply.getNode()).tell(new SetPredecessor(node), getSelf());

                    // The successor and predecessor of this node are now known
                    initialiseFirstFinger(findSuccessorReply.getNode());
                    generateFingerTable(getCentralNode(getCentralNodeAddress()));
                })
                .match(FindPredecessor.class, findPredecessor -> {
//                    log.info("FindPredecessor");
                    tellFindPredecessor(findPredecessor.getId(), findPredecessor.getIndex(), findPredecessor.getOriginalSender());
                })
                .match(FindPredecessorReply.class, findPredecessorReply -> {
                    log.info("FindPredecessorReply");
                    // Init finger table phase -> set first
                    if (fingerTableService.getFingers().isEmpty()) {
                        log.info("Init finger table phase -> set first");
                        /*
                        finger[1].node = n'.find_successor(finger[1].start);
                        predecessor = successor.predecessor;
                        successor predecessor;
                         */
                        fixLocalPredecessorAndSuccessor(findPredecessorReply);
                    }

                    // Init finger table phase -> finger table is not complete
                    else if (isFingerTableNotComplete()) {
                        log.info("Init finger table phase -> finger table is not complete");
                        /*
                        for i = 1 to m
                            if
                            else
                         */
                        loopAndFixFingers();
                    }

                    // Update others phase. The update message should be send by the original sender
                    // index (only given by update finger table) is not unset
                    else if (findPredecessorReply.getIndex() != FindPredecessorReply.UNSET) {
                        log.info("Update others phase. The update message should be send by the original sender: " + findPredecessorReply.getOriginalSender());
                        ActorSelection toUpdatePredRef = Util.getActorRef(getContext(), findPredecessorReply.getNode());
                        toUpdatePredRef.tell(new UpdateFinger(findPredecessorReply.getIndex(), node), getSelf());
                    }
                    // We are not in a phase that should happen.
                    else {
                        log.error("This shouldn't happen");
                    }
                })
                .match(SetPredecessor.class, setPredecessor -> {
                    log.info("I have a new predecessor: " + setPredecessor.getNode().getId());
                    fingerTableService.setPredecessor(setPredecessor.getNode());
                })
                .match(DirectGetSuccessor.class, directGetSuccessor -> {
//                    log.info("DirectGetSuccessor");
                    // Tell joining node that this is the successor he is searching for
                    getSender().tell(new FindSuccessorReply(fingerTableService.getSuccessor()), getSelf());
                })
                .match(UpdateFinger.class, updateFinger -> {
//                    log.info("UpdateFinger");
                    updateFingerTable(updateFinger.getNode(), updateFinger.getIndex());
                })
                //Send your predecessor upon a stabilize message from another node.
                .match(Stabilize.class, stabilize -> {
                    getSender().tell(new StabilizeReply(this.fingerTableService.getPredecessor()), getSelf());
                })
                //Set your new successor if your old successor has a new predecessor and notify your new successor that
                //you might be it's predecessor.
                .match(StabilizeReply.class, stabilizeReply -> {
                    ChordNode x = stabilizeReply.getPredecessor();
                    if (CompareUtil.between(node.getId(), false, this.fingerTableService.getSuccessor().getId(), false, x.getId())) { // FIXME should be between
                        this.fingerTableService.setSuccessor(x);
                    }
                    Util.getActorRef(getContext(), this.fingerTableService.getSuccessor()).tell(new Notify(this.node), getSelf());
                })
                //If you get notified of a possible (new) predecessor, check if this is the case and set it.
                .match(Notify.class, notify -> {
                    ChordNode possiblePredecessor = notify.getNode();
                    if (this.fingerTableService.getPredecessor() == null) {
                        this.fingerTableService.setPredecessor(possiblePredecessor);
                    } else if (CompareUtil.between(fingerTableService.getPredecessor().getId(), false, // FIXME should be between
                            this.node.getId(), false, possiblePredecessor.getId())) {
                        this.fingerTableService.setPredecessor(possiblePredecessor);
                    }
                })
                .match(FixFingers.class,
                        this::fixFingersPredecessor
                )
                .match(FixFingersReply.class,
                        this::handleFixFingersReply
                )
                .build();
    }

    private void joinNetwork() {
        final String nodeType = config.getString("myapp.nodeType");
        log.info("DEBUG -- nodetype: " + nodeType);
        if (nodeType.equals("regular")) {
            ActorSelection centralNode = getCentralNode(getCentralNodeAddress());

            // First step is to find the correct successor
            centralNode.tell(new FindSuccessor(fingerTableService.startFinger(node.getId(), 1)), getSelf());

        } else if (nodeType.equals("central")) {
            fingerTableService.setFingerTable(fingerTableService.initFingerTableCentral(node));
            fingerTableService.setPredecessor(node);
        }
    }

    private void fixLocalPredecessorAndSuccessor(FindPredecessorReply findPredecessorReply) {
        fingerTableService.setPredecessor(findPredecessorReply.getNode());
        ActorSelection predRef = Util.getActorRef(getContext(), findPredecessorReply.getNode());

        predRef.tell(new DirectGetSuccessor(), getSelf());

        // mySuccessor = predRef.getSuccessor

        // predRef.predecessor = node
    }

    private void initialiseFirstFinger(ChordNode successor) {
        long first = fingerTableService.startFinger(node.getId(), 1);
        long second = fingerTableService.startFinger(node.getId(), 2);
        FingerInterval firstInterval = new FingerInterval(first, second);
        fingerTableService.getFingers().add(new Finger(first, firstInterval, successor));
    }

    private void generateFingerTable(ActorSelection centralNode) {
        long beginFinger = fingerTableService.startFinger(node.getId(), generateFingerCount + 1);
        long nextFinger = fingerTableService.startFinger(node.getId(), generateFingerCount + 2);
        ChordNode currentSuccessor = fingerTableService.getFingers().get(generateFingerCount - 1).getSucc();

        FingerInterval interval = fingerTableService.calcInterval(beginFinger, nextFinger);
        if (nextFinger >= node.getId() && nextFinger < currentSuccessor.getId()) { // FIXME should be between
            fingerTableService.getFingers().add(new Finger(beginFinger, interval, currentSuccessor));
//            log.info("Sending to my self: " + getSelf().toString());

            getSelf().tell(new FindSuccessor(beginFinger), getSelf());
        } else {
            // Set empty successor will be handled when we get a message back
            fingerTableService.getFingers().add(new Finger(beginFinger, interval, null));

            // Ask for successor if not in current interval
            centralNode.tell(new FindSuccessor(nextFinger), getSelf());
        }
    }

    private void loopAndFixFingers() {
//        log.info("Init finger table phase finger table is not complete");
        fingerTableService.getFingers().get(generateFingerCount)
                .setSucc(fingerTableService.getSuccessor());

        generateFingerCount++;
        if (generateFingerCount < ChordStart.m) {
            generateFingerTable(getCentralNode(getCentralNodeAddress()));
        } else {
//            log.info("Update other nodes finger table should now be complete");
            printFingerTable();

            updateOthers();
        }
    }

    //Send a stabilize message to your current successor.
    private void stabilize() {
        Util.getActorRef(getContext(), fingerTableService.getSuccessor()).tell(new Stabilize(), getSelf());
    }

    //Pick a random finger table entry to refresh and send a FixFingers command to yourself.
    private void fixFingers() {
        Random r = new Random();
        int fingerSize = fingerTableService.getFingers().size();
        int i = r.nextInt(fingerSize);
        Finger finger = fingerTableService.getFingers().get(i);
        if (finger == null) {
            log.info("No finger entry at index {0}", i);
            return;
        }
        getSelf().tell(new FixFingers(i, finger.getStart()), getSelf());
    }

    private void handleFixFingersReply(FixFingersReply fingersReply) {
        int index = fingersReply.getIndex();
        Finger finger = this.fingerTableService.getFingers().get(index);
        fingerTableService.getFingers().set(index, new Finger(finger.getStart(), finger.getInterval(), fingersReply.getSuccessor()));
    }

    private void fixFingersPredecessor(FixFingers fixFingers) {
        long id = fixFingers.getStart();
        // If not in my interval
        if (!CompareUtil.between(node.getId(), false, fingerTableService.getSuccessor().getId(), true, id)) { // FIXME should be between
            // Find closest preceding finger in my finger table
            ChordNode closestNode = closestPrecedingFinger(id);
            ActorSelection closestNodeRef = Util.getActorRef(getContext(), closestNode);

            // Tell him to return his predecessor
            closestNodeRef.forward(new FixFingers(fixFingers.getIndex(), id), getContext());
        } else {
            // If I'm the node return my successor
            getSender().tell(new FixFingersReply(fingerTableService.getSuccessor(), fixFingers.getIndex()), getSelf());
        }
    }

    private void tellFindPredecessor(long id, int index, ActorRef originalSender) {
//        ChordNode n = closestPrecedingFinger(id);
        // If not in my interval
        if (!CompareUtil.between(node.getId(), false, fingerTableService.getSuccessor().getId(), true, id)) { // FIXME should be between
            log.info(id + " not in my interval");
            // Find closest preceding finger in my finger table
            ChordNode predecessor = closestPrecedingFinger(id);
            log.info(predecessor.getId() + " is the closestPrecedingFinger");
            ActorSelection closestPredNode = Util.getActorRef(getContext(), predecessor);

            // Tell him to return his predecessor
            log.info("Telling the closestPrecedingFinger to find predecessor: " + id + " index was: " + index);
            closestPredNode.tell(new FindPredecessor(id, index, originalSender), getSelf());
//            closestPredNode.forward(new FindPredecessor(id, index), getContext());
        } else {
            // If I'm the node return me to the original sender
            log.info("I'm the predecessor of " + id + " telling this to " + originalSender.toString());
            originalSender.tell(new FindPredecessorReply(node, index, getSender()), getSelf());
        }
        printFingerTable();
    }

    private ChordNode closestPrecedingFinger(long id) {
        for (int i = ChordStart.m; i >= 1; i--) {

            // Is in interval?
            long ithSucc = fingerTableService.getFingers().get(i - 1).getSucc().getId();
            if (CompareUtil.between(node.getId(), false, id, false, ithSucc)) { // FIXME should be between
                return fingerTableService.getFingers().get(i - 1).getSucc();
            }
        }
        // Return self
        return node;
    }

    private void updateOthers() {
        for (int i = 1; i <= ChordStart.m; i++) {
            tellFindPredecessor(getFingerWhoseIthFingerMightBeNode(i), i, getSelf());
        }
    }

    private long getFingerWhoseIthFingerMightBeNode(int i) {
        return Math.floorMod((long) (node.getId() - Math.pow(2, (i - 1))), ChordStart.AMOUNT_OF_KEYS);
    }

    private void updateFingerTable(ChordNode inNode, int index) {
        // In interval up to successor but not including the successor
        int adjustedIndex = index - 1;
        long fingerSuccId = fingerTableService.getFingers().get(adjustedIndex).getSucc().getId();
        if (CompareUtil.between(node.getId(), true, fingerSuccId, false, inNode.getId())) {
            if (inNode.getId() != node.getId()) {
                // Update my finger table
                fingerTableService.getFingers().get(adjustedIndex).setSucc(inNode);
                log.info("My finger table has been updated");
            }
            ActorSelection actorRef = Util.getActorRef(getContext(), fingerTableService.getPredecessor());
            actorRef.tell(new UpdateFinger(index, inNode), getSelf());
        } else {
            log.info("My finger table has not been updated");
        }
        printFingerTable();
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        log.info("Shutting down...");
    }

    // Util methods from here on

    private boolean isFingerTableNotComplete() {
        return fingerTableService.getFingers().stream().anyMatch(finger -> finger.getSucc() == null);
    }

    private void printFingerTable() {
        log.info(fingerTableService.toString());
    }

    private ActorSelection getCentralNode(String centralNodeAddress) {
        return getContext().actorSelection(centralNodeAddress);
    }

    private String getCentralNodeAddress() {
        final String centralEntityAddress = config.getString("myapp.centralEntityAddress");
        return "akka://ChordNetwork@" + centralEntityAddress + "/user/ChordActor";
    }
}
