package org.distributed.systems;

import akka.actor.ActorSystem;
import akka.actor.Props;
import org.distributed.systems.chord.actors.NodeActor;

public class ChordStart {

    public static final int STANDARD_TIME_OUT = 10000;
    // FIXME 160 according to sha-1 but this is the max_length of a java long..
    public static int M = 10; // Number of bits in key nodeId's
    public static final long AMOUNT_OF_KEYS = Math.round(Math.pow(2, M));

    public static void main(String[] args) {
        // Create actor system
        ActorSystem system = ActorSystem.create("ChordNetwork"); // Setup actor system
        // FIXME we want to read this value from the config but it doesn't work (yet)
        M = system.settings().config().getInt("myapp.mBits"); // Override mBits based on config

        system.actorOf(Props.create(NodeActor.class), "ChordActor");
    }
}
