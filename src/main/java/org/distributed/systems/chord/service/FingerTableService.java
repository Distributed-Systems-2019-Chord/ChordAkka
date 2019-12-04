package org.distributed.systems.chord.service;

import org.distributed.systems.ChordStart;
import org.distributed.systems.chord.model.ChordNode;
import org.distributed.systems.chord.model.finger.Finger;
import org.distributed.systems.chord.model.finger.FingerInterval;
import org.distributed.systems.chord.model.finger.FingerTable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FingerTableService {

    private FingerTable fingerTable;

    private ChordNode predecessor;

    public FingerTableService() {
        this.fingerTable = new FingerTable(new ArrayList<>(), 0);
    }

    public long startFinger(long nodeId, int fingerTableIndex) {
        return (long) ((nodeId + Math.pow(2, (fingerTableIndex - 1))) % ChordStart.AMOUNT_OF_KEYS);
    }

    public void setFingerTable(FingerTable fingerTable) {
        this.fingerTable = fingerTable;
    }

//    public FingerTable initFingerTable(ChordNode node) {
//        FingerTable table = new FingerTable(new ArrayList<>(ChordStart.m), 0);
//        for (int i = 1; i <= ChordStart.m; i++) {
//            long startFinger = startFinger(node.getId(), i);
//            long endFinger = startFinger(node.getId(), i + 1);
//            FingerInterval interval = calcInterval(startFinger, endFinger);
//            table.addFinger(new Finger(startFinger, interval, calcSuccessor()));
//        }
//        return table;
//    }

    public void initFingerTableRegular(long id){
        for(int i=1;i<=ChordStart.m;i++){
            long startFinger = startFinger(id,i);
            long endFinger = startFinger(id,i+1);
            FingerInterval interval = calcInterval(startFinger, endFinger);
            this.fingerTable.addFinger(new Finger(startFinger, interval, null));
        }
    }
    public void initFingerTableCentral(ChordNode centralNode) {
        for (int i = 1; i <= ChordStart.m; i++) {
            long startFinger = startFinger(centralNode.getId(), i);
            long endFinger = startFinger(centralNode.getId(), i + 1);
            FingerInterval interval = calcInterval(startFinger, endFinger);
            this.fingerTable.addFinger(new Finger(startFinger, interval, centralNode));
        }
        System.out.println("Finger table for central node generated size is " + this.fingerTable.getFingerList().size() + " should be " + ChordStart.m);
    }

    public FingerInterval calcInterval(long start1, long start2) {
        long startIndex = start1 % ChordStart.AMOUNT_OF_KEYS;
        long endIndex = start2 % ChordStart.AMOUNT_OF_KEYS;

        return new FingerInterval(startIndex, endIndex);
    }

    public List<Finger> getFingers() {
        return this.fingerTable.getFingerList();
    }

    public void setSuccessor(ChordNode successor) {
        fingerTable.getFingerList().get(0).setSucc(successor);
    }

    public ChordNode getSuccessor() {
        if (fingerTable.getFingerList().isEmpty()) {
            return null;
        }
        return fingerTable.getFingerList().get(0).getSucc();
    }

    public void setPredecessor(ChordNode predecessor) {
        this.predecessor = predecessor;
    }

    public ChordNode getPredecessor() {
        return predecessor;
    }

    @Override
    public String toString() {
        return "\n"
                + "Predecessor: " + predecessor.getId() + "\n"
                + "Successor: " + getSuccessor().getId() + "\n"
                + "FINGER TABLE:\n"
                + getFingers().stream().map(Finger::toString)
                .map(s -> "\t" + s + "\n").collect(Collectors.joining());
    }
}
