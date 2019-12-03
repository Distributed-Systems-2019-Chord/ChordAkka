package org.distributed.systems.chord.messaging;

import akka.actor.ActorRef;

import java.io.Serializable;

public class FindSuccessor {

    public static class Request implements Command, Serializable {
        public final long id;
        public final long fingerTableIndex;

        public Request(Long id, long fingerTableIndex) {
            this.id = id;
            this.fingerTableIndex = fingerTableIndex;
        }
    }

    public static class Reply implements Response {

        public final ActorRef succesor;
        public final long id;
        public final long fingerTableIndex;

        public Reply(ActorRef successor, long id, long fingerTableIndex ) {
            this.succesor = successor;
            this.id = id;
            this.fingerTableIndex = fingerTableIndex;
        }
    }
}
