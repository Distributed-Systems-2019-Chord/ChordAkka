package org.distributed.systems.chord.messaging;

import akka.actor.ActorRef;

import java.io.Serializable;

public class Notify {
    public static class Request implements Command, Serializable {

        public ActorRef ndashActorRef;
        public long ndashId;

        public Request(ActorRef ndashActorRef, long ndashId) {
            this.ndashActorRef = ndashActorRef;
            this.ndashId = ndashId;
        }
    }

    public static class Reply implements Response {
        public Reply() {

        }
    }
}
