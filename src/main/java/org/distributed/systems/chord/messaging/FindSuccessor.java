package org.distributed.systems.chord.messaging;

import akka.actor.ActorRef;

public class FindSuccessor {

    public static class Request implements Command {
        public final long id;
        public final int fingerTableIndex;

        public Request(long id, int fingerTableIndex) {
            this.id = id;
            this.fingerTableIndex = fingerTableIndex;
        }
    }

    public static class Reply implements Response {

        public final ActorRef succesor;
        public final long id;
        public final int fingerTableIndex;

        public Reply(ActorRef successor, long id, int fingerTableIndex) {
            this.succesor = successor;
            this.id = id;
            this.fingerTableIndex = fingerTableIndex;
        }
    }
}
