package org.distributed.systems.chord.messaging;

import java.io.Serializable;

public class KeyValue {

    public static class Put implements Command {
        public final String originalKey;
        public final long hashKey;
        public final Serializable value;


        public Put(String originalKey, long hashKey, Serializable value) {
            this.originalKey = originalKey;
            this.value = value;
            this.hashKey = hashKey;
        }
    }

    public static class PutReply implements Response {
        private final String originalKey;
        public final long hashKey;
        private final Serializable value;

        public PutReply(String originalKey, long hashKey, Serializable value) {
            this.originalKey = originalKey;
            this.hashKey = hashKey;
            this.value = value;
        }
    }

    public static class Get implements Command {
        public final String originalKey;
        public final long hashKey;

        public Get(String originalKey, long hashKey) {
            this.originalKey = originalKey;
            this.hashKey = hashKey;
        }
    }

    public static class GetReply implements Response {

        public final String originalKey;
        public final long hashKey;
        public final Serializable value;

        public GetReply(String originalKey, long hashKey, Serializable value) {
            this.originalKey = originalKey;
            this.hashKey = hashKey;
            this.value = value;
        }
    }
}
