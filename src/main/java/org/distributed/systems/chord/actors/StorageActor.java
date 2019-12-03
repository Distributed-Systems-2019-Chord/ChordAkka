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
                    long key = putValueMessage.key;
                    Serializable value = putValueMessage.value;
                    log.info("Put for key, value: " + key + " " + value);
                    this.storageService.put(key, value);

                    ActorRef optionalSender = getContext().getSender();
                    if (optionalSender != getContext().getSystem().deadLetters()) {
                        optionalSender.tell(new KeyValue.PutReply(), ActorRef.noSender());
                    }

                })
                .match(KeyValue.Get.class, getValueMessage -> {
                    Serializable val = this.storageService.get(getValueMessage.key);
                    getContext().getSender().tell(new KeyValue.GetReply(val), ActorRef.noSender());
                })
                .build();
    }
}
