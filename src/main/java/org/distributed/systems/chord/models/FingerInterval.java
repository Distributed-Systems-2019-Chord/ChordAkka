package org.distributed.systems.chord.models;


// Only used to print the interval for the finger table
public class FingerInterval {

    private final long start;
    private final long end;

    public FingerInterval(long start, long end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString() {
        return start + "\t-\t" + end;
    }
}
