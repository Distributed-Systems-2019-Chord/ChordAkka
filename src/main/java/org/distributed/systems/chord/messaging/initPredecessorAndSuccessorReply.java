package org.distributed.systems.chord.messaging;

import org.distributed.systems.chord.model.ChordNode;

public class initPredecessorAndSuccessorReply extends FindPredecessorAndSuccessorReply {
    public initPredecessorAndSuccessorReply(ChordNode predecessor, ChordNode successor){
        super(predecessor,successor);
    }
}
