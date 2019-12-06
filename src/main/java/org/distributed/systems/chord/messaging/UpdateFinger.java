package org.distributed.systems.chord.messaging;

import org.distributed.systems.chord.models.ChordNode;

public class UpdateFinger {

    public static class Request implements Command {
        public final int fingerTableIndex;
        public final ChordNode chordNode;

        public Request(int fingerTableIndex, ChordNode chordNode) {
            this.fingerTableIndex = fingerTableIndex;
            this.chordNode = chordNode;
        }
    }

}
