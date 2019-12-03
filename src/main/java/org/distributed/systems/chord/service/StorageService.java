package org.distributed.systems.chord.service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class StorageService {

    private Map<Long, Serializable> valueStore;

    public StorageService() {
        this.valueStore = new HashMap<>();
    }

    public void put(long key, Serializable value) {
        this.valueStore.put(key, value);
    }

    public Serializable get(long key) {
        return this.valueStore.get(key);
    }

}
