package org.distributed.systems.chord.models;

public class Pair<L,R> {

    private final L originalKey;
    private final R value;

    public Pair(L originalKey, R value) {
        this.originalKey = originalKey;
        this.value = value;
    }

    public L getOriginalKey() { return originalKey; }
    public R getValue() { return value; }

    @Override
    public int hashCode() { return originalKey.hashCode() ^ value.hashCode(); }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Pair)) return false;
        Pair pairo = (Pair) o;
        return this.originalKey.equals(pairo.getOriginalKey()) &&
                this.value.equals(pairo.getValue());
    }

}