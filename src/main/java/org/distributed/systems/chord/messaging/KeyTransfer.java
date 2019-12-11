package org.distributed.systems.chord.messaging;

import akka.actor.ActorRef;

import java.util.List;

public class KeyTransfer {

    public static class Request implements Command {
        public final ActorRef successor;
        public final List<Long> keys;

        public Request(ActorRef successor, List<Long> keys) {
            this.successor = successor;
            this.keys = keys;
        }
    }
}
