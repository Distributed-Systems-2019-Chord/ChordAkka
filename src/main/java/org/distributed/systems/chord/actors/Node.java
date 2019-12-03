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
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Random;

public class Node extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
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
        Random randomGenerator = new Random();
        long randomInt = randomGenerator.nextInt(ChordStart.M);
        this.id = randomInt;
        System.out.println("Node Id " + randomInt);

        System.out.println(this.type);

        if (this.type.equals("central")) {
            //Assumption: central node first up and never fails :D
            // thus: node initially has it's pre and suc equal it's own identity
            this.predecessor = getSelf();
            this.predecessorId = this.id;
            this.sucessor = getSelf();
            this.sucessorId = this.id;

            System.out.println("I am a central node, so I don't care about others");
        } else {
            // Assumption: Central node is on ip 127.0.0.1:25521 and is asked to join
            final String centralNodeAddress = getCentralNodeAddress();
            Timeout timeout = Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT));
            System.out.println("I am a regular node, so I need my central");
            Future<ActorRef> centralNodeFuture = getContext().actorSelection(centralNodeAddress).resolveOne(timeout);
            this.centralNode = (ActorRef) Await.result(centralNodeFuture, timeout.duration());
            System.out.println("I found my central node");
        }

        if (this.centralNode != null) {
            // Request a Join
            JoinMessage.JoinRequest joinRequestMessage = new JoinMessage.JoinRequest(getSelf(), this.id);
            this.centralNode.tell(joinRequestMessage, getSelf());
            // We need something, that ensures if this message get's lost --> everything from here on relies on msgs i guess
        }
    }

    private void getValueForKey(long key) {
        if (shouldKeyBeOnThisNodeOtherwiseForward(key, new KeyValue.Get(key))) {
            Serializable getResult = getValueFromStorageActor(key);
            System.out.println("I got this result on my get lookup: " + getResult);
        }
    }

    private Serializable getValueFromStorageActor(long key) {
        KeyValue.Get getRequest = new KeyValue.Get(key);
        Timeout timeout = Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT));
        Future<Object> valueResponse = Patterns.ask(this.storageActorRef, getRequest, timeout);
        try {
            return ((KeyValue.GetReply) Await.result(valueResponse, timeout.duration())).value;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private void putValueForKey(long key, Serializable value) {
        if (shouldKeyBeOnThisNodeOtherwiseForward(key, new KeyValue.Put(key, value))) {
            putValueInStore(key, value);
            System.out.println("I DID PUT VALUE " + value + " ON NODE " + id + " THE KEY WAS " + key);

            System.out.println("GETTING THE VALUE NOW..");

            getValueForKey(key);
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

                    if (this.predecessor == getSelf() && this.sucessor == getSelf() && this.type == "regular") {
                        JoinMessage.JoinReply joinReplyMessage = new JoinMessage.JoinReply(null, null, false);
                        msg.requestor.tell(joinReplyMessage, getSelf());
                        System.out.println("I declined the JOIN, I am a regular Node being part of no network");
                        return;
                    }

                    if (msg.requestorKey == this.id) {
                        JoinMessage.JoinReply joinReplyMessage = new JoinMessage.JoinReply(null, null, false);
                        msg.requestor.tell(joinReplyMessage, getSelf());
                        System.out.println("I declined the JOIN, node that request join has same key!");
                        return;
                    }
                    // Central Node Is The Only Node in the ring
                    if (this.predecessor == getSelf() && this.sucessor == getSelf()) {
                        // Easy Case: Just Join as Predecessor + Sucessor
                        this.sucessor = msg.requestor;
                        this.sucessorId = msg.requestorKey;
                        this.predecessor = msg.requestor;
                        this.predecessorId = msg.requestorKey;
                        JoinMessage.JoinReply joinReplyMessage = new JoinMessage.JoinReply(getSelf(), getSelf(), true, this.id, this.id);
                        msg.requestor.tell(joinReplyMessage, getSelf());
                    } else {
                        // FOUR cases:
                        // 1. Smaller than predecessor -> forward this message
                        // 2. Between predecessor and me --> ask predecessor for join confirmation and then I reply okay
                        // 3. Between sucessor and me --> ask sucessor for join confirmation and then I reply okay
                        // 4. Greater than successor -> forward this message
                        if (msg.requestorKey < this.predecessorId) {
                            // this.predecessor.forward(msg, getContext());
                            System.out.println("I forward to predecessor!");

                            // if my predecessor is bigger than i am, then the joiner needs to inserted here -> else forward
                            if (this.id < this.predecessorId) {
                                // TODO: Code Duplicate
                                handleJoinRefPred(msg);
                                return;

                            } else {
                                this.predecessor.forward(msg, getContext());
                            }

                            return;
                        } else if (this.predecessorId < msg.requestorKey && msg.requestorKey < this.id) {
                            handleJoinRefPred(msg);
                        } else if (this.id < msg.requestorKey && msg.requestorKey < this.sucessorId) {
                            handleJoinRefSucc(msg);
                            return;
                        } else if (this.sucessorId < msg.requestorKey) {
                            System.out.println("I forward to sucessor!");
                            // this.sucessor.forward(msg, getContext());
                            // if my sucessor is smaller than i am, then the joiner needs to inserted here -> else forward
                            if (this.sucessorId < this.id) {
                                // TODO: Code Duplicate
                                handleJoinRefSucc(msg);

                            } else {
                                this.sucessor.forward(msg, getContext());
                            }

                            return;
                        } else {
                            // Else: Keys are equal: Reject join
                            JoinMessage.JoinReply joinReplyMessage = new JoinMessage.JoinReply(null, null, false);
                            msg.requestor.tell(joinReplyMessage, getSelf());
                            System.out.println("I declined the JOIN, node that request join has same key of a node in the network!");
                            return;
                        }
                    }
                    System.out.println("I accepted the JOIN");
                    System.out.println("Successor:" + this.sucessor.toString() + " with id:" + this.sucessorId);
                    System.out.println("Predecessor:" + this.predecessor.toString() + " with id:" + this.predecessorId);
                })
                .match(JoinMessage.JoinReply.class, msg -> {
                    System.out.println("I got a JoinReply");
                    if (msg.accepted) {
                        this.predecessor = msg.predecessor;
                        this.predecessorId = msg.predecessorId;
                        this.sucessor = msg.sucessor;
                        this.sucessorId = msg.sucessorId;
                        System.out.println("I joined the Network");
                        System.out.println("Successor:" + this.sucessor.toString() + " with id:" + this.sucessorId);
                        System.out.println("Predecessor:" + this.predecessor.toString() + " with id:" + this.predecessorId);


                        // If success then we can put and ask key values
                        putValueForKey(0, 12);
                        putValueForKey(30, 42);
                    } else {
                        System.out.println("Could not JOIN network, Shutting down");
                        getContext().stop(getSelf());
                    }
                })
                .match(JoinMessage.JoinConfirmationRequest.class, msg -> {
                    System.out.println("I need to confirm a join request");

                    // Determine if a successor or predecessor
                    if (msg.newPredecessor == null && msg.newSucessor != null) {
                        // it's a successor
                        this.sucessorId = msg.newSucessorKey;
                        this.sucessor = msg.newSucessor;
                        JoinMessage.JoinConfirmationReply confirmReplyMsg = new JoinMessage.JoinConfirmationReply(true);
                        getContext().getSender().tell(confirmReplyMsg, ActorRef.noSender());
                        System.out.println("I confirmed the join -- ");
                        System.out.println("New successor: " + msg.newSucessorKey);
                    } else if (msg.newPredecessor != null && msg.newSucessor == null) {
                        // it's a predecessor
                        this.predecessorId = msg.newPredecessorKey;
                        this.predecessor = msg.newPredecessor;
                        JoinMessage.JoinConfirmationReply confirmReplyMsg = new JoinMessage.JoinConfirmationReply(true);
                        getContext().getSender().tell(confirmReplyMsg, ActorRef.noSender());
                        System.out.println("I confirmed the join -- ");
                        System.out.println("New predecessor: " + msg.newPredecessorKey);
                    } else {
                        // it's equal -> need to reject this!
                        JoinMessage.JoinConfirmationReply confirmReplyMsg = new JoinMessage.JoinConfirmationReply(false);
                        getContext().getSender().tell(confirmReplyMsg, ActorRef.noSender());
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
                    ActorRef memcacheHandler = getContext().actorOf(Props.create(MemcachedActor.class, storageActorRef = this.storageActorRef));
                    getSender().tell(TcpMessage.register(memcacheHandler), getSelf());
                })
                .match(KeyValue.Put.class, putValueMessage -> {
                    long key = putValueMessage.key;
                    Serializable value = putValueMessage.value;
                    putValueForKey(key, value);
//                    log.info("key, value: " + key + " " + value);
//                    this.storageActorRef.forward(putValueMessage, getContext());
                })
                .match(KeyValue.Get.class, getValueMessage -> {
                    getValueForKey(getValueMessage.key);
//                    this.storageActorRef.forward(getValueMessage, getContext());
                })
                .build();
    }

    private void handleJoinRefSucc(JoinMessage.JoinRequest msg) throws Exception {
        JoinMessage.JoinConfirmationRequest joinConfirmationRequestMessage = new JoinMessage.JoinConfirmationRequest(msg.requestor, msg.requestorKey, null, 0);
        Timeout timeout = Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT));
        Future<Object> confirmationReqFuture = Patterns.ask(this.sucessor, joinConfirmationRequestMessage, timeout);
        JoinMessage.JoinConfirmationReply result = (JoinMessage.JoinConfirmationReply) Await.result(confirmationReqFuture, timeout.duration());
        //TODO: Handle timeout!
        if (result.accepted) {
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

    private void handleJoinRefPred(JoinMessage.JoinRequest msg) throws Exception {
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
        // TODO: We need to expose this port to the outer world
        // Get possible hostname:
        String hostname = "localhost";

        if (System.getenv("HOSTNAME") != null) {
            hostname = System.getenv("HOSTNAME");
        }


        InetSocketAddress tcp_socked = new InetSocketAddress(hostname, port);
        Tcp.Command tcpmsg = TcpMessage.bind(getSelf(), tcp_socked, 100);
        tcp.tell(tcpmsg, getSelf());
    }

}
