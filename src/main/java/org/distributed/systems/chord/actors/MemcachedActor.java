package org.distributed.systems.chord.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.util.ByteString;
import akka.util.Timeout;
import org.distributed.systems.ChordStart;
import org.distributed.systems.chord.messaging.KeyValue;
import scala.concurrent.Await;
import scala.concurrent.Future;
import org.distributed.systems.chord.util.impl.HashUtil;

import java.util.Arrays;

class MemcachedActor extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final ActorRef node;
    private String previousTextCommand = "";
    private HashUtil hashUtil;

    private ActorRef client;

    public MemcachedActor(ActorRef node) {
        this.node = node;
        this.hashUtil = new HashUtil();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Tcp.Received.class, msg -> {
                    client = getSender();
                    final ByteString data = msg.data();
                    String request = data.decodeString("utf-8");
                    processMemcachedRequest(request);
                })
                .match(Tcp.ConnectionClosed.class, msg -> {
                    getContext().stop(getSelf());
                })
                .match(KeyValue.PutReply.class, putReply -> {
//                    System.out.println("OK I got PUTREPLY");

                    ByteString resp = ByteString.fromString("STORED\r\n");
                    client.tell(TcpMessage.write(resp), getSelf());
                })
                .match(KeyValue.GetReply.class, getReply -> {
//                    System.out.println("OK I got GETREPLY");


                    //FIXME Isn't received by the client
//                    System.out.println("fetched payload");
                    int payload_length = getReply.value.toString().length();
                    ByteString getdataresp = ByteString.fromString(getReply.value.toString());
                    // 99 is unique id
                    ByteString getresp = ByteString.fromString("VALUE " + getReply.key + "  " + (payload_length) + "\r\n");
                    client.tell(TcpMessage.write(getresp), getSelf());
                    client.tell(TcpMessage.write(getdataresp), getSelf());
                })
                .build();
    }

    private void processMemcachedRequest(String request) {
        // Process each line that is passed:
        String[] textCommandLines = request.split("\r\n");
//        System.out.println(Arrays.toString(textCommandLines));

        for (String textCommand : textCommandLines) {
            if (textCommand.startsWith("get") && !previousTextCommand.startsWith("set")) {
                handleGetCommand(textCommand);
            } else if (textCommand.startsWith("set")) {
                // do nothing, go to payload
            } else if (textCommand.startsWith("quit")) {
                handleQuitCommand();
            } else {
                if (previousTextCommand.startsWith("set")) {
                    handleSetCommand(textCommand);
                } else {
                    System.out.println("Unknown Query Command");
                }
            }
            this.previousTextCommand = textCommand;
        }

//        System.out.println("Handled A MemCache Request");
    }

    private void handleGetCommand(String commandLine) {
        try {
            String[] get_options = commandLine.split(" ");
            long key = hashUtil.hash(get_options[1]);
//            System.out.println("Fetching payload");
            KeyValue.Get keyValueGetMessage = new KeyValue.Get(key);
            node.tell(keyValueGetMessage, getSelf());
        } catch (Exception e) {
            // TODO: how handle exception
            e.printStackTrace();
        }

    }

    private void handleSetCommand(String payloadTextLine) {
        try {
            String[] set_options = previousTextCommand.split(" ");
            // TODO: @Stijn -> You get strings like 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
            // those need to be mapped to a node -> and the storageActor should actually store this string key so memcslamp can return them
            long hashKey = hashUtil.hash(set_options[1]);
            KeyValue.Put putValueMessage = new KeyValue.Put(hashKey, payloadTextLine);
            node.tell(putValueMessage, getSelf());
        } catch (Exception e) {
            // TODO: how handle exception
            e.printStackTrace();
        }
    }

    private void handleQuitCommand() {
        getSender().tell(TcpMessage.close(), getSelf());
        System.out.println("Closed A MemCache Connection");
    }
}
