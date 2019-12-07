package org.distributed.systems.chord.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.distributed.systems.chord.messaging.KeyValue;
import org.distributed.systems.chord.service.StorageService;

import java.io.Serializable;

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
                    Serializable value = putValueMessage.value;
                    this.storageService.put(putValueMessage.originalKey, value);

                    ActorRef optionalSender = getContext().getSender();
                    if (optionalSender != getContext().getSystem().deadLetters()) {
                        optionalSender.tell(new KeyValue.PutReply(putValueMessage.originalKey, hashKey, value), ActorRef.noSender());
                    }

                })
                .match(KeyValue.Get.class, getValueMessage -> {
                    long key = getValueMessage.hashKey;
                    Serializable val = this.storageService.get(getValueMessage.originalKey);
                    getContext().getSender().tell(new KeyValue.GetReply(getValueMessage.originalKey, key, val), ActorRef.noSender());
                })
                .build();
    }
}
