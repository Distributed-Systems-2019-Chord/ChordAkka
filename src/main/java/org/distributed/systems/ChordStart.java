package org.distributed.systems;

import akka.actor.ActorSystem;
import akka.actor.Props;
import org.distributed.systems.chord.actors.Node;

public class ChordStart {

    public static final int STANDARD_TIME_OUT = 5000;
    // FIXME 160 according to sha-1 but this is the max_length of a java long..
    public static final int M = 64;

    public static void main(String[] args) {
        // Create actor system
        ActorSystem system = ActorSystem.create("ChordNetwork"); // Setup actor system

        system.actorOf(Props.create(Node.class), "ChordActor");
    }
}
