package org.distributed.systems.chord.messaging;

import org.distributed.systems.chord.models.ChordNode;

import java.util.List;

public class SuccessorList {

    public static class Request implements Command {
        public Request() {
        }
    }

    public static class Reply implements Response {
        public final List<ChordNode> successorList;

        public Reply(List<ChordNode> successorList) {
            this.successorList = successorList;
        }

        public Reply() {
            this.successorList = null;
        }
    }
}
