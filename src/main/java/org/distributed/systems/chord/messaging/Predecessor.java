package org.distributed.systems.chord.messaging;

import org.distributed.systems.chord.models.ChordNode;

import java.util.List;

public class Predecessor {
    public static class Request implements Command {
        public final List<Long> succList;
        public Request(List<Long> succList) {
            this.succList = succList;
        }
    }

    public static class Reply implements Response {
        public ChordNode predecessor;
        public final List<ChordNode> succList;

        public Reply(ChordNode predecessor,  List<ChordNode> succList) {
            this.predecessor = predecessor;
            this.succList = succList;
        }
    }
}
