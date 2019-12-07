package org.distributed.systems.chord.messaging;

public class Ping {

    public static class Request implements Command {
        public Request() {

        }
    }

    public static class Reply implements Response {
        public Reply(){

        }
    }
}
