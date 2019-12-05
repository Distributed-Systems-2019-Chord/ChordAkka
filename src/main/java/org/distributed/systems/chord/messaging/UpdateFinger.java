package org.distributed.systems.chord.messaging;

import org.distributed.systems.chord.actors.Node;

public class UpdateFinger {

    public static class Request implements Command {
        public final int fingerTableIndex;
        public final Node.FingerTableEntry fingerTableEntry;

        public Request(int fingerTableIndex, Node.FingerTableEntry fingerTableEntry) {
            this.fingerTableIndex = fingerTableIndex;
            this.fingerTableEntry = fingerTableEntry;
        }
    }

}
