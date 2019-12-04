package org.distributed.systems.chord.messaging;

public class FindPredecessorAndSuccessor implements Command {
    private final long id;
    private FindPredecessorAndSuccessorReply reply;

    public FindPredecessorAndSuccessor(long id, FindPredecessorAndSuccessorReply reply) {
        this.id = id;
        this.reply = reply;
    }

    public long getId() {
        return id;
    }

    public FindPredecessorAndSuccessorReply getReply() {
        return this.reply;
    }

}
