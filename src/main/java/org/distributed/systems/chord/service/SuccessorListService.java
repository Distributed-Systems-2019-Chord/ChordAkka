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

    public void replaceAll(ChordNode nodeToReplace, ChordNode newNode){
        this.successorList.stream().map(s -> s.id == nodeToReplace.id ? newNode: s).collect(Collectors.toList());
    }

    public void prependEntry(ChordNode entry){
        successorList.add(0, entry);

        if(successorList.size() > r)
            removeLastEntry();
    }

    public ChordNode findFirstLiveEntry(Long idFilter){
        ChordNode n = this.successorList.stream().filter(s -> s.id != idFilter).findFirst().orElse(null);
        return n;
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
