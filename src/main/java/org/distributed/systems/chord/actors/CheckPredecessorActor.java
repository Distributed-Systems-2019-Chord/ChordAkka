package org.distributed.systems.chord.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import org.distributed.systems.chord.messaging.CheckPredecessor;

public class CheckPredecessorActor extends AbstractActor {
    ActorRef nodeActor;
    public CheckPredecessorActor(ActorRef nodeActor) {
        this.nodeActor = nodeActor;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals(
                        "CheckPredecessor",
                        m -> {
                            nodeActor.tell(new CheckPredecessor.Request(), getSelf());
                        })
                .build();
    }
}
