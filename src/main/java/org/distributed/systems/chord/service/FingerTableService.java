package org.distributed.systems.chord.service;

import org.distributed.systems.ChordStart;
import org.distributed.systems.chord.models.ChordNode;
import org.distributed.systems.chord.models.FingerInterval;
import org.distributed.systems.chord.models.FingerTableEntry;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FingerTableService {

    public final int NOT_SET = -1;

    private static FingerTableEntry[] fingerTable;

    private ChordNode predecessor;

    public FingerTableService(long nodeId) {
        fingerTable = new FingerTableEntry[ChordStart.M];

        initEmptyFingerTable(nodeId);

        setSuccessor(new ChordNode(NOT_SET, null));
        setPredecessor(new ChordNode(NOT_SET, null));
    }

    private void initEmptyFingerTable(long nodeId) {
        for (int i = 1; i <= ChordStart.M; i++) {
            long start = startFinger(nodeId, i);
            FingerInterval interval = calcInterval(start, startFinger(nodeId, i + 1));
            fingerTable[i - 1] = new FingerTableEntry(start, interval, null);
        }
    }

    public void setSuccessor(ChordNode finger) {
        setFingerEntryForIndex(0, finger);
    }

    public void setFingerEntryForIndex(int index, ChordNode finger) {
        fingerTable[index].setSuccessor(finger);
    }

    public ChordNode getEntryForIndex(int index) {
        return fingerTable[index].getSuccessor();
    }

    public ChordNode getSuccessor() {
        return getEntryForIndex(0);
    }

    public void setPredecessor(ChordNode predecessor) {
        this.predecessor = predecessor;
    }

    public ChordNode getPredecessor() {
        return predecessor;
    }

    public void printFingerTable(boolean override) {
        if (ChordStart.ENABLE_LOGGING || override) {
            System.out.println(toString());
        }
    }

    @Override
    public String toString() {
        String fingers =
                IntStream.range(0, fingerTable.length)
                        .boxed()
                        .map(fingerIndex -> {
                            ChordNode succFinger = fingerTable[fingerIndex].getSuccessor();
                            if (succFinger == null) {
                                return "empty\n";
                            } else {
                                return fingerTable[fingerIndex].getStart() + "\t|\t" + fingerTable[fingerIndex].getInterval().toString() + "\t\t|\t" + succFinger.toString() + "\n";
                            }
                        }).collect(Collectors.joining());

        return "FINGER TABLE: \n"
                + "Predecessor: " + getPredecessor().id + "\n"
                + "Successor: " + getSuccessor().id + "\n"
//                + "START" + "\t|\t" + "INTERVAL" + "\t|\t" + "SUCCESSOR" + "\n"
                + fingers;
    }

    public long startFinger(long nodeId, int fingerTableIndex) {
        return (long) ((nodeId + Math.pow(2, (fingerTableIndex - 1))) % ChordStart.AMOUNT_OF_KEYS);
    }

    public FingerInterval calcInterval(long start1, long start2) {
        long startIndex = start1 % ChordStart.AMOUNT_OF_KEYS;
        long endIndex = start2 % ChordStart.AMOUNT_OF_KEYS;

        return new FingerInterval(startIndex, endIndex);
    }
}
