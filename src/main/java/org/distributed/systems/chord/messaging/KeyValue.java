package org.distributed.systems.chord.messaging;

import org.distributed.systems.chord.models.Pair;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class KeyValue {

    public static class Put implements Command {
        public final long hashKey;
        public final Pair<String, Serializable> value;


        public Put(long hashKey, Pair<String, Serializable> value) {
            this.value = value;
            this.hashKey = hashKey;
        }
    }

    public static class PutReply implements Response {
        public final long hashKey;
        public final Pair<String, Serializable> value;

        public PutReply(long hashKey, Pair<String, Serializable> value) {
            this.hashKey = hashKey;
            this.value = value;
        }
    }
    public static class Get implements Command {
        public final long hashKey;

        public Get(long hashKey) {
            this.hashKey = hashKey;
        }
    }
    public static class GetAll implements Command {

        public GetAll() {

        }
    }

    public static class GetAllReply implements Response {
        public final Map<Long, Pair<String, Serializable>> keys;

        public GetAllReply(Map<Long, Pair<String, Serializable>> keys) {
            this.keys = keys;
        }
    }

    public static class GetReply implements Response {

        public final long hashKey;
        public final Pair<String, Serializable> value;

        public GetReply(long hashKey, Pair<String, Serializable> value) {
            this.hashKey = hashKey;
            this.value = value;
        }
    }

    public static class GetSubset implements Response{
        public final List<Long> keys;

        public GetSubset(List<Long> keys) {
            this.keys = keys;
        }
    }

    public static class GetSubsetReply implements Response{
        public final Map<Long, Pair<String, Serializable>> keyValues;

        public GetSubsetReply(Map<Long, Pair<String, Serializable>> keyValues) {
            this.keyValues = keyValues;
        }
    }

    public static class DeleteSubset implements Response{
        public final List<Long> keys;

        public DeleteSubset(List<Long> keys) {
            this.keys = keys;
        }
    }

    public static class DeleteSubsetReply implements Response{

        public DeleteSubsetReply() {

        }
    }

    //TODO: delete original key
    public static class Delete implements  Command {
        public final String originalKey;
        public final long hashKey;

        public Delete(String originalKey, long hashKey) {
            this.originalKey = originalKey;
            this.hashKey = hashKey;
        }
    }

    public static class DeleteReply implements Response {

        public  DeleteReply() { }

    }
}
