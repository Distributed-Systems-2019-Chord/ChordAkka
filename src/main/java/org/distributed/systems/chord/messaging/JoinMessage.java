package org.distributed.systems.chord.messaging;

import akka.actor.ActorRef;

public class JoinMessage {

    public static class JoinRequest implements Command {

        public final long requestorKey;
        public final ActorRef requestor;

        public JoinRequest(ActorRef requestor, long requestorKey) {
            this.requestor = requestor;
            this.requestorKey = requestorKey;
        }
    }
}
