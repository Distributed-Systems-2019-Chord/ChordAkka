package org.distributed.systems.chord.messaging;

import org.distributed.systems.chord.actors.Node;

import java.io.Serializable;

public class UpdateFinger {

    public static class Request implements Command {
        public final long fingerTableIndex;
        public final Node.FingerTableEntry fingerTableEntry;

        public Request(Long fingerTableIndex, Node.FingerTableEntry fingerTableEntry) {
            this.fingerTableIndex = fingerTableIndex;
            this.fingerTableEntry = fingerTableEntry;
        }
    }

}
