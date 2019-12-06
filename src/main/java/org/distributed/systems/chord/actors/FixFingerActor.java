package org.distributed.systems.chord.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import org.distributed.systems.chord.messaging.FixFingers;

class FixFingerActor extends AbstractActor {
    ActorRef nodeActor;
    public FixFingerActor(ActorRef nodeActor) {
        this.nodeActor = nodeActor;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals(
                        "FixFinger",
                        m -> {
                            nodeActor.tell(new FixFingers.Request(), getSelf());
                        })
                .build();
    }
}