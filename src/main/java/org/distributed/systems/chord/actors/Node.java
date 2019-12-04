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
                .match(SetPredecessor.class, setPredecessor -> {
                    //Set my new predecessor
                    this.fingerTableService.setPredecessor(setPredecessor.getNode());
                    log.info("Set my predecessor to "+setPredecessor.getNode().getId());
                })
                .match(FindPredecessorAndSuccessor.class, findPredecessorAndSuccessor -> {

                    long id = findPredecessorAndSuccessor.getId();
                    //If you are the predecessor
                    if(CompareUtil.between(this.node.getId(),false,fingerTableService.getSuccessor().getId(),true,id,true)){
                        //Set the successor and predecessor in the reply
                        findPredecessorAndSuccessor.getReply().setPredecessor(this.node);
                        findPredecessorAndSuccessor.getReply().setSuccessor(this.fingerTableService.getSuccessor());

                        //Tell the original sender its predecessor and successor.
                        getSender().tell(findPredecessorAndSuccessor.getReply(),getSelf());
                    }
                    //If not
                    else{
                        //Find closest preceding finger
                        ChordNode nPrime = closestPrecedingFinger(id);
                        //Forward the find predecessor and successor message to him
                        Util.getActorRef(getContext(),nPrime).forward(findPredecessorAndSuccessor,getContext());
                    }
                })
                .match(FindPredecessorAndSuccessorReply.class, reply -> {
                    this.fingerTableService.setSuccessor(reply.getSuccessor());
                })
                /*
                .match(initPredecessorAndSuccessorReply.class, reply -> {
                    //Set my successor and predecessor
                    this.fingerTableService.setSuccessor(reply.getSuccessor());
                    this.fingerTableService.setPredecessor(reply.getPredecessor());

                    log.info("Set my successor to" + reply.getSuccessor().getId()+" and my Predecessor to "+ reply.getPredecessor().getId());
                    //Tell my successor I'm his new predecessor
                    Util.getActorRef(getContext(),this.fingerTableService.getSuccessor()).tell(new SetPredecessor(this.node),getSelf());

                    //Start the initFingerTableLoop
                    initFingerTableLoop(1);
                })
                .match(initLoopReply.class, reply ->{
                    fingerTableService.getFingers().get(reply.getIndex()).setSucc(reply.getSuccessor());
                    printFingerTable();
                    log.info("index is "+reply.getIndex());
                    //If done
                    if(reply.getIndex()==ChordStart.m -1){
                        log.info("Starting update others");
                        //Start to update others
                        updateOthers(1);
                    }
                    //Else continue
                    else {
                        initFingerTableLoop(reply.getIndex() + 1);
                    }
                })
                .match(updateOthersReply.class, reply ->{
                    log.info("Telling " +reply.getPredecessor().getId() +
                            " to update finger table with id"+this.node.getId()+ " and index "+reply.getIndex());

                    if(reply.getPredecessor().getId()==node.getId()){
                        Util.getActorRef(getContext(),fingerTableService.getPredecessor()).tell(new UpdateFingerTable(this.node,reply.getIndex()),getSelf());
                    }
                    else{
                        //Tell the found predecessor to update his finger table.
                        Util.getActorRef(getContext(),reply.getPredecessor()).tell(new UpdateFingerTable(this.node,reply.getIndex()),getSelf());
                    }

                    //If not done yet, continue updating others
                    if(reply.getIndex()<ChordStart.m){
                        updateOthers(reply.getIndex() + 1);
                    }
                })
                .match(UpdateFingerTable.class, updateFingerTable -> {
                    int adjustedIndex = updateFingerTable.getIndex() -1;
                    ChordNode iNode = this.fingerTableService.getFingers().get(adjustedIndex).getSucc();
                    long start = this.fingerTableService.startFinger(node.getId(),adjustedIndex);
                    boolean lowerBound = false;
                    if(node.getId()<iNode.getId()){
                        lowerBound = true;
                    }
                    ChordNode s = updateFingerTable.getNode();
                    log.info("Received an update finger table with id "+s.getId()+" and index " + updateFingerTable.getIndex() + " from" + getSender());
                    log.info("iNode is "+iNode.getId());
                    if(CompareUtil.between(this.node.getId(),lowerBound       ,iNode.getId(),
                            false,s.getId(),true)
                            &&iNode.getId()!=fingerTableService.getFingers().get(adjustedIndex).getStart()){
                        log.info(s.getId() + " is between " + this.node.getId() +" and " + iNode.getId());
                        log.info("My finger table has been updated");
                        fingerTableService.getFingers().get(adjustedIndex).setSucc(s);
                        printFingerTable();
                        //Tell my predecessor to also update his finger table.
                        Util.getActorRef(getContext(),fingerTableService.getPredecessor()).tell(
                                new UpdateFingerTable(s,updateFingerTable.getIndex()),getSelf());
                    }
                    else{
                        log.info(s.getId() + " is NOT between " + this.node.getId() +" and " + iNode.getId());
                        log.info("Denied an update finger table");
                        printFingerTable();
                    }
                })
*/
                //Send your predecessor upon a stabilize message from another node.
                .match(Stabilize.class, stabilize -> {
                    getSender().tell(new StabilizeReply(this.fingerTableService.getPredecessor()), getSelf());
                })
                //Set your new successor if your old successor has a new predecessor and notify your new successor that
                //you might be it's predecessor.
                .match(StabilizeReply.class, stabilizeReply -> {
                    ChordNode x = stabilizeReply.getPredecessor();
                    if (CompareUtil.between(node.getId(), false, this.fingerTableService.getSuccessor().getId(), false, x.getId(),true)) { // FIXME should be between
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
                            this.node.getId(), false, possiblePredecessor.getId(),true)) {
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
            fingerTableService.initFingerTableRegular(node.getId());
            ActorSelection centralNode = getCentralNode(getCentralNodeAddress());
            centralNode.tell(new FindPredecessorAndSuccessor(node.getId(),new FindPredecessorAndSuccessorReply(null,null)),getSelf());
            //centralNode.tell(new FindPredecessorAndSuccessor(this.fingerTableService.startFinger(node.getId(),1)
             //       ,new initPredecessorAndSuccessorReply(null,null)),getSelf());
            // First step is to find the correct successor

        } else if (nodeType.equals("central")) {
            fingerTableService.initFingerTableCentral(this.node);
            fingerTableService.setPredecessor(node);
        }
    }

    private void initFingerTableLoop(int index){
        log.info("Init finger table loop with i = " + index);
        for(int i = index; i<ChordStart.m;i++){
            int adjustedIndex = i-1;
            long start = fingerTableService.getFingers().get(adjustedIndex+1).getStart();
            ChordNode iSucc = fingerTableService.getFingers().get(adjustedIndex).getSucc();
            if(CompareUtil.between(node.getId(),true,iSucc.getId(),false,start,true)){
                fingerTableService.getFingers().get(adjustedIndex + 1).setSucc(iSucc);
                printFingerTable();
                if(i==ChordStart.m -1){
                    log.info("Start updating others");
                    updateOthers(1);
                }
            }
            else{
                getCentralNode(getCentralNodeAddress()).tell(new FindPredecessorAndSuccessor(start, new initLoopReply(null,null, i)),getSelf());
                break;
            }
        }
    }

    private void updateOthers(int index) {
        long id = node.getId() - (long)Math.pow(2,index-1);
        getSelf().tell(new FindPredecessorAndSuccessor(id, new updateOthersReply(null,null,index)),getSelf());
    }

    private static class Runnable implements java.lang.Runnable{

        @Override
        public void run() {
            while(true){
            }
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
        if (!CompareUtil.between(node.getId(), false, fingerTableService.getSuccessor().getId(), true, id,true)) { // FIXME should be between
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



    private ChordNode closestPrecedingFinger(long id) {
        for (int i = ChordStart.m; i >= 1; i--) {

            // Is in interval?
            long ithSucc = fingerTableService.getFingers().get(i - 1).getSucc().getId();
            if (CompareUtil.between(node.getId(), false, id, false, ithSucc,true)) { // FIXME should be between
                return fingerTableService.getFingers().get(i - 1).getSucc();
            }
        }
        // Return self
        return node;
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
