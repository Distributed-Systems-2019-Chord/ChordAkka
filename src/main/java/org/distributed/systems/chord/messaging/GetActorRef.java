package org.distributed.systems.chord.messaging;

import akka.actor.ActorRef;

public class GetActorRef {

    public static class Request implements Command {

        public Request() {

        }
    }

    public static class Reply implements Command {
        public ActorRef storageActor;

        public Reply(ActorRef storageActor) {
            this.storageActor = storageActor;
        }
    }
}
