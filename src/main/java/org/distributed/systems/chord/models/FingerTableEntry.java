package org.distributed.systems.chord.models;

public class FingerTableEntry {

    private Long start;
    private FingerInterval interval;
    private ChordNode successor;

    public FingerTableEntry(Long start, FingerInterval interval, ChordNode successor) {
        this.start = start;
        this.interval = interval;
        this.successor = successor;
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public FingerInterval getInterval() {
        return interval;
    }

    public void setInterval(FingerInterval interval) {
        this.interval = interval;
    }

    public ChordNode getSuccessor() {
        return successor;
    }

    public void setSuccessor(ChordNode successor) {
        this.successor = successor;
    }

    @Override
    public String toString() {
        return start + " | " + interval.toString() + " | " + successor;
    }
}
