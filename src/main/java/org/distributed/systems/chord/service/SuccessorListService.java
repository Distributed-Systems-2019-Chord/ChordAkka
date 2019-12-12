package org.distributed.systems.chord.service;

import org.distributed.systems.chord.models.ChordNode;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SuccessorListService {

    public static final short r = 3;
    private static List<ChordNode> successorList;

    public SuccessorListService(){
        this.successorList = new ArrayList<ChordNode>();
    }


    public void removeLastEntry(){
        if (this.successorList.size() > 0)
            this.successorList.remove(successorList.size() -1);
    }

    public void prependEntry(ChordNode entry){
        successorList.add(0, entry);

        if(successorList.size() > r)
            removeLastEntry();
    }

    public ChordNode findFirstLiveEntry(){
        return this.successorList.get(1);
    }

    public ChordNode getSuccessor(short i){
        return successorList.get(i);
    }

    public void setList(List<ChordNode> newSuccessorList){
        this.successorList = new ArrayList<ChordNode>(newSuccessorList);
    }

    public List<ChordNode> getList(){
        return this.successorList;
    }

    public List<Long> getAllButFirst(){
        return this.successorList.subList(1, this.successorList.size()-1).stream().map(s -> s.id).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (ChordNode cn : successorList)
        {
            sb.append(cn.id);
            sb.append("\n");
        }
        return sb.toString();
    }
}
