package org.distributed.systems.chord.messaging;

import org.distributed.systems.chord.actors.Node;

import java.io.Serializable;

public class Ping {

    public static class Request implements Command, Serializable {

        private String trace;

        public void appendTrace(long id) {
            trace = trace + " -> " + id;
        }

        @Override
        public String toString() {
            return trace.toString();
        }

        public Request(long id) {
            this.trace = ".. " + id;
        }
    }
}
