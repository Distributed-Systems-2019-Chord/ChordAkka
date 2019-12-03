package org.distributed.systems.chord.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.util.ByteString;
import org.distributed.systems.chord.messaging.KeyValue;

import java.util.Arrays;

class MemcachedActor extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final ActorRef node;
    private String previousTextCommand = "";
    private final ActorRef storageActor;

    private ActorRef client;

    public MemcachedActor(ActorRef storageActor, ActorRef node) {
        this.storageActor = storageActor;
        this.node = node;
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
                    System.out.println("OK I got PUTREPLY");

                    ByteString resp = ByteString.fromString("STORED\r\n");
                    client.tell(TcpMessage.write(resp), getSelf());
                })
                .match(KeyValue.GetReply.class, getReply -> {
                    System.out.println("OK I got GETREPLY");


                    //FIXME Isn't received by the client
                    System.out.println("fetched payload");
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
        System.out.println(Arrays.toString(textCommandLines));

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

        System.out.println("Handled A MemCache Request");
    }

    private void handleGetCommand(String commandLine) {
        try {
            String[] get_options = commandLine.split(" ");
            long key = Long.valueOf(get_options[1]);
            System.out.println("Fetching payload");
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
            long hashKey = Long.valueOf(set_options[1]);
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
