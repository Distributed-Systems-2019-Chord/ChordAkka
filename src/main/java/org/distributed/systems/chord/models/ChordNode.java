package org.distributed.systems.chord.models;

import akka.actor.ActorRef;

import java.io.Serializable;

public class ChordNode implements Serializable {

    public Long id;

    // Is IP + Port
    public ActorRef chordRef;

    public ChordNode(long id, ActorRef chordRef) {
        this.id = id;
        this.chordRef = chordRef;
    }

    @Override
    public String toString() {
        return id.toString();
    }
}