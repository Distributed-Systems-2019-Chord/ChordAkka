package org.distributed.systems.chord.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.pattern.Patterns;
import akka.util.ByteString;
import org.distributed.systems.chord.messaging.KeyValue;
import scala.concurrent.Await;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;

import scala.concurrent.Future;
import akka.util.Timeout;

class MemcachedActor extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private static final int STANDARD_TIME_OUT = 1000;
    private String previousTextCommand = "";
    private final ActorRef storageActor;

    public MemcachedActor(ActorRef storageActor) {
        this.storageActor = storageActor;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Tcp.Received.class, msg -> {
                    final ByteString data = msg.data();
                    String request = data.decodeString("utf-8");
                    processMemcachedRequest(request);
                })
                .match(Tcp.ConnectionClosed.class, msg -> {
                    getContext().stop(getSelf());
                })
                .build();
    }

    private void processMemcachedRequest(String request) {
        // Process each line that is passed:
        String[] textCommandLines = request.split("\r\n");
        System.out.println(Arrays.toString(textCommandLines));

        for (String textCommand : textCommandLines) {
            if (textCommand.startsWith("get")) {
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
            String key = get_options[1];
            System.out.println("Fetching payload");

            KeyValue.Get keyValueGetMessage = new KeyValue.Get(key);
            Timeout timeout = Timeout.create(Duration.ofMillis(STANDARD_TIME_OUT));
            Future<Object> future = Patterns.ask(this.storageActor, keyValueGetMessage, timeout);
            KeyValue.GetReply result = (KeyValue.GetReply) Await.result(future, timeout.duration());

            System.out.println("Response for get value: " + result.value.toString());
            Serializable payload = result.value;
            if (payload == null) {
                System.out.println("NON payload");
            } else {
                System.out.println("fetched payload");
                Integer payload_length = payload.toString().length();
                ByteString getdataresp = ByteString.fromString(payload.toString() + "\r\n");
                // 99 is unique id
                ByteString getresp = ByteString.fromString("VALUE " + key + "  " + (payload_length) + " 99\r\n");

                getSender().tell(TcpMessage.write(getresp), getSelf());
                getSender().tell(TcpMessage.write(getdataresp), getSelf());
            }
        } catch (Exception e) {
            // TODO: how handle exception
            e.printStackTrace();
        }

    }

    private void handleSetCommand(String payloadTextLine) {
        try {
            String[] set_options = previousTextCommand.split(" ");
            String hashKey = set_options[1];
            KeyValue.Put putValueMessage = new KeyValue.Put(hashKey, payloadTextLine);

            Timeout timeout = Timeout.create(Duration.ofMillis(STANDARD_TIME_OUT));
            Future<Object> future = Patterns.ask(this.storageActor, putValueMessage, timeout);
            Await.result(future, timeout.duration());
            ByteString resp = ByteString.fromString("STORED\r\n");
            getSender().tell(TcpMessage.write(resp), getSelf());
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
