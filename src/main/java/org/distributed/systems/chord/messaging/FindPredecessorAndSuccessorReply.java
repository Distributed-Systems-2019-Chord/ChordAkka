package org.distributed.systems.chord.messaging;

import akka.actor.ActorRef;
import org.distributed.systems.chord.model.ChordNode;

public class FindPredecessorAndSuccessorReply implements Response {

    private ChordNode predecessor;
    private ChordNode successor;

    public FindPredecessorAndSuccessorReply(ChordNode predecessor, ChordNode successor) {
        this.predecessor = predecessor;
        this.successor = successor;
    }

    public ChordNode getPredecessor() {
        return this.predecessor;
    }

    public ChordNode getSuccessor() {
        return this.successor;
    }

    public void setPredecessor(ChordNode predecessor) {
        this.predecessor = predecessor;
    }

    public void setSuccessor(ChordNode successor) {
        this.successor = successor;
    }
}
