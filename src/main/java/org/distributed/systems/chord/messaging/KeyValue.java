package org.distributed.systems.chord.messaging;

import java.io.Serializable;

public class KeyValue {

    public static class Put implements Command {
        public final long key;
        public final Serializable value;


        public Put(long key, Serializable value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class PutReply implements Response {
        private final long key;
        private final Serializable value;

        public PutReply(long key, Serializable value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class Get implements Command {
        public final long key;

        public Get(long key) {
            this.key = key;
        }
    }

    public static class GetReply implements Response {

        public final long key;

        public final Serializable value;

        public GetReply(long key, Serializable value) {
            this.key = key;
            this.value = value;
        }
    }
}
