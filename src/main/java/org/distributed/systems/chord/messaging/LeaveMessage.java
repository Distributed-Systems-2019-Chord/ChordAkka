package org.distributed.systems.chord.messaging;

import org.distributed.systems.chord.models.ChordNode;
import org.distributed.systems.chord.models.Pair;

import java.io.Serializable;
import java.util.Map;

public class LeaveMessage {

    public static class Request implements  Command {

    }

    public static class ForPredecessor implements Command {
        private ChordNode successor;
    public ForPredecessor(ChordNode successor){
        this.successor = successor;
    }
    public ChordNode getSuccessor(){
        return this.successor;
    }
    }
    public static class ForSuccessor implements Command {

        private ChordNode predecessor;
        private Map<Long, Pair<String, Serializable>> keyValues;

        public ForSuccessor(ChordNode predecessor, Map<Long, Pair<String, Serializable>> keyValues){
            this.predecessor = predecessor;
            this.keyValues = keyValues;
        }
        public ChordNode getPredecessor(){
            return this.predecessor;
        }
        public Map<Long, Pair<String, Serializable>> getKeyValues() {
            return keyValues;
        }
    }
}
