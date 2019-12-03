package org.distributed.systems.chord.messaging;

import java.io.Serializable;

public class Print {
    public static class Request implements Command, Serializable {
        public Request() {
        }
    }
}
