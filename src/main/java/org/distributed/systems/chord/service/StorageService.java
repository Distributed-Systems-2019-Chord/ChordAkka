package org.distributed.systems.chord.service;

import org.distributed.systems.chord.models.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StorageService {

    private Map<Long, Pair<String, Serializable>> valueStore;

    public StorageService() {
        this.valueStore = new HashMap<>();
    }

    public void put(long key, Pair<String, Serializable> value) {
        this.valueStore.put(key, value);
    }

    public Pair<String, Serializable> get(long key) {
        return this.valueStore.get(key);
    }

    public Map<Long, Pair<String,Serializable>> getAll(){
        return this.valueStore;
    }

    public void putAll(Map<Long, Pair<String, Serializable>> valueStore) {
        this.valueStore.putAll(valueStore);
    }

    public Map<Long, Pair<String, Serializable>> getSubset(List<Long> keys) {
        return keys.stream()
                .filter(this.valueStore::containsKey)
                .collect(Collectors.toMap(Function.identity(), this.valueStore::get));
    }

    public void deleteSubset(List<Long> keys) {
        keys.stream()
                .filter(this.valueStore::containsKey)
                .map(this.valueStore::remove);
    }

    public void delete(Long key) {
        this.valueStore.remove(key);
    }
}
