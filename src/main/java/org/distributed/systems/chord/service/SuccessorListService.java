package org.distributed.systems.chord.service;

import org.distributed.systems.chord.models.ChordNode;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SuccessorListService {

    public static final short r = 3;
    private List<ChordNode> successorList;

    public SuccessorListService() {
        this.successorList = new ArrayList<>();
    }


    public void removeLastEntry() {
        if (successorList.size() > 0)
            successorList.remove(successorList.size() - 1);
    }

    public void prependEntry(ChordNode entry) {
        successorList.add(0, entry);

        if (successorList.size() > r)
            removeLastEntry();
    }

    public void setList(List<ChordNode> newSuccessorList) {
        successorList = newSuccessorList;
    }

    public List<ChordNode> getList() {
        return this.successorList;
    }

    public List<Long> getAllButFirst() {
        if (successorList.isEmpty()) {
            return new ArrayList<>();
        } else {
            return successorList.subList(1, successorList.size())
                    .stream()
                    .map(s -> s.id)
                    .collect(Collectors.toList());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (ChordNode cn : successorList) {
            sb.append(cn.id);
            sb.append("\n");
        }
        return sb.toString();
    }
}
