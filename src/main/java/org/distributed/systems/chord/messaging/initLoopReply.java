package org.distributed.systems.chord.messaging;

import org.distributed.systems.chord.model.ChordNode;

public class initLoopReply extends FindPredecessorAndSuccessorReply {
    private  int index;

    public initLoopReply(ChordNode predecessor, ChordNode successor, int index){
        super(predecessor,successor);
        this.index = index;
    }
    public int getIndex(){
        return this.index;
    }
}
