package org.distributed.systems.chord.messaging;

import akka.actor.ActorRef;

import java.io.Serializable;

public class FixFingers {

    public static class Request implements Command, Serializable {
        public Request() {

        }
    }

}
