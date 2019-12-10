package org.distributed.systems.chord.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.dispatch.Futures;
import akka.dispatch.OnComplete;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.pattern.Patterns;
import akka.util.ByteString;
import akka.util.Timeout;
import org.distributed.systems.ChordStart;
import org.distributed.systems.chord.messaging.KeyValue;
import org.distributed.systems.chord.models.Pair;
import org.distributed.systems.chord.util.impl.HashUtil;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.time.Duration;
import java.util.ArrayList;

import static akka.dispatch.Futures.future;

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
                .build();
    }

    private void processMemcachedRequest(String request) {
        // Process each line that is passed:
        String[] textCommandLines = request.split("\r\n");
        ArrayList<Future<Boolean>> queries = new ArrayList<>();
        for (String textCommand : textCommandLines) {
            if (textCommand.startsWith("get") && !previousTextCommand.startsWith("set")) {

                String[] get_options = textCommand.split(" ");
                String key = get_options[1].trim();
                long hashKey = this.hashUtil.hash(key);
                KeyValue.Get keyValueGetMessage = new KeyValue.Get(hashKey);

                Future<Boolean> f = future(() -> {
                    // Await this future, and then return another future
                    // This is done, to answer queries as soon as possible!
                    Timeout timeout = Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT));
                    Future<Object> queryFuture = Patterns.ask(node, keyValueGetMessage, timeout);
                    try {
                        KeyValue.GetReply rply = (KeyValue.GetReply) Await.result(queryFuture, timeout.duration());

                        if (rply.value != null) {
                            int payload_length = rply.value.toString().length();
                            ByteString getdataresp = ByteString.fromString(rply.value.getValue().toString() + "\r\n");
                            ByteString getresp = ByteString.fromString("VALUE " + key + " 0 " + (payload_length) + " \r\n");
                            client.tell(TcpMessage.write(getresp), getSelf());
                            client.tell(TcpMessage.write(getdataresp), getSelf());

                            // Signal that query finished:
                        } else {
                            // Ignore as not found
                        }
                        client.tell(TcpMessage.write(ByteString.fromString("END\r\n")), getSelf());
                        return Boolean.TRUE;
                    } catch (Exception e) {
                        // Write Some Error Notice to Memcache
                        System.out.println(e);
                        return Boolean.FALSE;
                    }
                }, getContext().getSystem().getDispatcher());
                queries.add(f);

            } else if (textCommand.startsWith("set")) {
                // do nothing, go to payload
            } else if (textCommand.startsWith("delete")) {
                // like this delete <key> [noreply]\r\n
                String[] delete_options = textCommand.split(" ");
                String key = delete_options[1].trim();
                long hashKey = this.hashUtil.hash(key);
                KeyValue.Delete deleteValueMessage = new KeyValue.Delete(key, hashKey);
                Future<Boolean> f = future(() -> {
                    // Await this future, and then return another future
                    // This is done, to answer queries as soon as possible!
                    Timeout timeout = Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT));
                    Future<Object> queryFuture = Patterns.ask(node, deleteValueMessage, timeout);
                    try {
                        KeyValue.DeleteReply rply = (KeyValue.DeleteReply) Await.result(queryFuture, timeout.duration());
                        ByteString resp = ByteString.fromString("DELETED\r\n");

                        client.tell(TcpMessage.write(resp), getSelf());
                        return Boolean.TRUE;
                    } catch (Exception e) {
                        // Write Some Error Notice to Memcache
                        System.out.println("MemCache Delete Error: " + e.toString());
                        return Boolean.FALSE;
                    }
                }, getContext().getSystem().getDispatcher());
                queries.add(f);

            } else if (textCommand.startsWith("quit")) {
                getSender().tell(TcpMessage.close(), getSelf());
            } else {
                if (previousTextCommand.startsWith("set")) {
                    String[] set_options = previousTextCommand.split(" ");
                    String key = set_options[1].trim();
                    long hashKey = this.hashUtil.hash(key);
                    KeyValue.Put putValueMessage = new KeyValue.Put(hashKey, new Pair(key, textCommand));

                    Future<Boolean> f = future(() -> {
                        // Await this future, and then return another future
                        // This is done, to answer queries as soon as possible!
                        Timeout timeout = Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT));
                        Future<Object> queryFuture = Patterns.ask(node, putValueMessage, timeout);
                        try {
                            KeyValue.PutReply rply = (KeyValue.PutReply) Await.result(queryFuture, timeout.duration());
                            ByteString resp = ByteString.fromString("STORED\r\n");
                            client.tell(TcpMessage.write(resp), getSelf());
                            return Boolean.TRUE;
                        } catch (Exception e) {
                            // Write Some Error Notice to Memcache
                            System.out.println("MemCache Error: " + e.toString());
                            return Boolean.FALSE;
                        }
                    }, getContext().getSystem().getDispatcher());
                    queries.add(f);
                } else {
                    System.out.println("Unknown Query Command");
                }
            }
            this.previousTextCommand = textCommand;
        }

        Future<Iterable<Boolean>> seq = Futures.sequence(queries, getContext().getSystem().dispatcher());
        seq.onComplete(
                new OnComplete<Iterable<Boolean>>() {
                    public void onComplete(Throwable failure, Iterable<Boolean> queryResults) {
                        // Hook for finished query
                    }
                }, getContext().system().dispatcher());

    }

}
