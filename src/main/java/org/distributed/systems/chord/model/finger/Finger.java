package org.distributed.systems.chord.model.finger;

import org.distributed.systems.chord.model.ChordNode;

public class Finger {

    private long start; // key

    private FingerInterval interval;

    private ChordNode succ; // node ref

    public Finger(long start, FingerInterval interval, ChordNode succ) {
//        index missing?
        this.start = start;
        this.interval = interval;
        this.succ = succ;
    }

    public long getStart() {
        return start;
    }

    public FingerInterval getInterval() {
        return interval;
    }

    public void setSucc(ChordNode succ) {
        this.succ = succ;
    }

    public ChordNode getSucc() {
        return succ;
    }

    @Override
    public String toString() {
        if (succ != null) {
            return start + "|" + interval.getStartKey() + "-" + interval.getEndKey() + "|" + succ.getId();
        } else {
            return start + "|" + interval.getStartKey() + "-" + interval.getEndKey() + "|" + "NULL - not set";
        }
    }
}
