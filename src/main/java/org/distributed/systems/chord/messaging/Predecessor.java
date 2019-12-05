package org.distributed.systems.chord.messaging;

import akka.actor.ActorRef;

import java.io.Serializable;

public class Predecessor {
    public static class Request implements Command {
        public Request() {

        }
    }

    public static class Reply implements Response {
        public ActorRef predecessor;
        public long predecessorId;

        public Reply(ActorRef predecessor, long predecessorId) {
            this.predecessor = predecessor;
            this.predecessorId = predecessorId;
        }
    }
}
