package org.distributed.systems.chord.messaging;

import java.io.Serializable;

public class Stabelize {

    public static class Request implements Command, Serializable {
        public Request() {

        }
    }

    public static class Reply implements Response {
        public Reply() {

        }
    }
}
