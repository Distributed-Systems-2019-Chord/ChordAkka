package org.distributed.systems.chord.messaging;

import java.io.Serializable;

public class KeyValue {

    public static class Put implements Command {
        public final String key;
        public final Serializable value;


        public Put(String key, Serializable value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class PutReply implements Response {
        public PutReply() {
        }
    }

    public static class Get implements Command {
        public final String key;

        public Get(String key) {
            this.key = key;
        }
    }

    public static class GetReply implements Response {

        public final Serializable value;

        public GetReply(Serializable value) {
            this.value = value;
        }
    }
}
