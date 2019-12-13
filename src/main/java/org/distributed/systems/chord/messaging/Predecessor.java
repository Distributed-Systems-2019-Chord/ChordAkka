package org.distributed.systems.chord.messaging;

import org.distributed.systems.chord.models.ChordNode;

public class Predecessor {
    public static class Request implements Command {
        public Request() {

        }
    }

    public static class Reply implements Response {
        public ChordNode predecessor;

        public Reply(ChordNode predecessor) {
            this.predecessor = predecessor;
        }
    }
}
