package org.distributed.systems.chord.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import org.distributed.systems.chord.messaging.Stabelize;

class StabilizeActor extends AbstractActor {
    ActorRef nodeActor;
    public StabilizeActor(ActorRef nodeActor) {
        this.nodeActor = nodeActor;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals(
                        "Stabilize",
                        m -> {
                            nodeActor.tell(new Stabelize.Request(), getSelf());
                        })
                .build();
    }
}