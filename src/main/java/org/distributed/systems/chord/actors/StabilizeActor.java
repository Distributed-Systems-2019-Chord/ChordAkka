package org.distributed.systems.chord.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import org.distributed.systems.chord.messaging.Stabilize;

class StabilizeActor extends AbstractActor {
    private ActorRef nodeActor;

    public StabilizeActor(ActorRef nodeActor) {
        this.nodeActor = nodeActor;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals(
                        "Stabilize",
                        m -> {
                            nodeActor.tell(new Stabilize.Request(), getSelf());
                        })
                .build();
    }
}