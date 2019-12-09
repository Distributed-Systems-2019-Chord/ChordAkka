package org.distributed.systems.chord.messaging;

import java.io.Serializable;
import java.lang.annotation.Repeatable;
import java.util.Map;

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
    public static class GetAll implements Command {

        public GetAll() {

        }
    }

    public static class GetAllReply implements Response {
        public final Map<String,Serializable> keys;

        public GetAllReply(Map<String,Serializable> keys) {
            this.keys = keys;
        }
    }

    public static class GetReply implements Response {

        public final Serializable value;

        public GetReply(Serializable value) {
            this.value = value;
        }
    }
}
