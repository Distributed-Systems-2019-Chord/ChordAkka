package org.distributed.systems.chord.messaging;

import akka.actor.ActorRef;

public class FindSuccessor {

    public static class Request implements Command {
        public final long id;
        public final int fingerTableIndex;
        public final long amountOfHops;

        public Request(long id, int fingerTableIndex, long amountOfHops) {
            this.id = id;
            this.fingerTableIndex = fingerTableIndex;
            this.amountOfHops = amountOfHops;
        }
    }

    public static class Reply implements Response {

        public final ActorRef succesor;
        public final long id;
        public final int fingerTableIndex;
        public final long amountOfHops;

        public Reply(ActorRef successor, long id, int fingerTableIndex, long amountOfHops) {
            this.succesor = successor;
            this.id = id;
            this.fingerTableIndex = fingerTableIndex;
            this.amountOfHops = amountOfHops;
        }
    }
}
