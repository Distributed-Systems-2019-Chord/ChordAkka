package org.distributed.systems.chord.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.distributed.systems.ChordStart;
import org.distributed.systems.chord.messaging.GetActorRef;
import org.distributed.systems.chord.messaging.KeyTransfer;
import org.distributed.systems.chord.messaging.KeyValue;
import org.distributed.systems.chord.models.Pair;
import org.distributed.systems.chord.service.StorageService;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.io.Serializable;
import java.util.Map;
import java.time.Duration;

class StorageActor extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private StorageService storageService;


    public StorageActor() {
        this.storageService = new StorageService();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(KeyValue.Put.class, putValueMessage -> {
                    long hashKey = putValueMessage.hashKey;
                    Pair<String, Serializable> value = putValueMessage.value;
                    System.out.println("Put key " + hashKey);
                    this.storageService.put(hashKey, value);

                    ActorRef optionalSender = getContext().getSender();
                    if (optionalSender != getContext().getSystem().deadLetters()) {
                        optionalSender.tell(new KeyValue.PutReply(hashKey, value), ActorRef.noSender());
                    }

                })
                .match(KeyValue.Get.class, getValueMessage -> {
                    long key = getValueMessage.hashKey;
                    System.out.println("GET key " + key);
                    Pair<String, Serializable> val = this.storageService.get(key);
                    getContext().getSender().tell(new KeyValue.GetReply(key, val), ActorRef.noSender());
                })
                .match(KeyTransfer.Request.class, keyTransferMessage ->{
                    Timeout timeoutTransfer = Timeout.create(Duration.ofMillis(ChordStart.STANDARD_TIME_OUT));

                    // first ask the storage actor ref of the successor

                    Future<Object> storageActorGetFuture = Patterns.ask(keyTransferMessage.successor, new GetActorRef.Request(), timeoutTransfer);
                    GetActorRef.Reply storageActorGetReply = (GetActorRef.Reply) Await.result(storageActorGetFuture, timeoutTransfer.duration());
                    // next, ask the successor's storage actor for the keys
                    Future<Object> getSubsetFuture = Patterns.ask(storageActorGetReply.storageActor, new KeyValue.GetSubset(keyTransferMessage.keys), timeoutTransfer);
                    KeyValue.GetSubsetReply getSubsetReply = (KeyValue.GetSubsetReply) Await.result(getSubsetFuture, timeoutTransfer.duration());
                    this.storageService.putAll(getSubsetReply.keyValues);

                    // todo call delete for keys (that contained a value)
                    storageActorGetReply.storageActor.tell(new KeyValue.DeleteSubset(keyTransferMessage.keys), getSelf());

                })
                .match(KeyValue.GetSubset.class, getSubsetMessage ->{
                    // Get subset from Key value store based on list of keys
                    getSender().tell(new KeyValue.GetSubsetReply(this.storageService.getSubset(getSubsetMessage.keys)), getSelf());
                })
                .match(KeyValue.DeleteSubset.class, deleteSubsetMessage ->{
                    // Delete subset from Key value store
                    this.storageService.deleteSubset(deleteSubsetMessage.keys);
                })
                .match(KeyValue.Delete.class, deleteMessage -> {
                    this.storageService.delete(deleteMessage.hashKey);
                    getContext().getSender().tell(new KeyValue.DeleteReply(), ActorRef.noSender());
                })
                .match(KeyValue.GetAll.class, getAllMessage->{
                    Map<String,Serializable> keyValues = this.storageService.getAll();
                    getContext().getSender().tell(new KeyValue.GetAllReply(keyValues), ActorRef.noSender());
                })
                .build();
    }
}
