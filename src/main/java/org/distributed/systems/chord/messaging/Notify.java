package org.distributed.systems.chord.messaging;

import org.distributed.systems.chord.models.ChordNode;

public class Notify {
    public static class Request implements Command {

        public final ChordNode nPrime;

        public Request(ChordNode nPrime) {
            this.nPrime = nPrime;
        }
    }

}
