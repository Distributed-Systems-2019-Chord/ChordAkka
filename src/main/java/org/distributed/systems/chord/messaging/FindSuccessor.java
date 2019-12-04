package org.distributed.systems.chord.messaging;

public class FindSuccessor implements Command {

    private final long id;
    private FindSuccessorReply reply;

    public FindSuccessor(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }
}
