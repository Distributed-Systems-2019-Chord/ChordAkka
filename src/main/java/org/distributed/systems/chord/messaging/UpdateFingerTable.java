package org.distributed.systems.chord.messaging;

import org.distributed.systems.chord.model.ChordNode;

public class UpdateFingerTable implements Command {

    private ChordNode node;
    private int index;
    public UpdateFingerTable(ChordNode node, int index){
        this.node = node;
        this.index = index;
    }

    public ChordNode getNode() {
        return node;
    }

    public int getIndex() {
        return index;
    }
}
