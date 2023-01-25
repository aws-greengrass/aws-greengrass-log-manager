package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logmanager.LogManagerService;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;


/**
 * LRU cache that holds information about each file that is being processed. As long as a log file is in the file system
 * and can be found, we can't know for sure if that file won't get new contents written to it in the future.
 */
public class ProcessingFileLRU extends LinkedHashMap<String, LogManagerService.CurrentProcessingFileInformation> {
    static final long serialVersionUID = 1L;
    private final int capacity;

    public ProcessingFileLRU(int capacity) {
        super(5, 0.75f, true);
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry eldest) {
        return size() > capacity;
    }

    /**
     * Converts the objects stored in the LRY into a map. Used serialize the LRU to be stored
     * on the runtime config
     */
    public Map<String, Object> toMap() {
        HashMap<String, Object> map = new HashMap<>();

        this.forEach((key, value) -> {
            map.put(key, value.convertToMapOfObjects());
        });

        return map;
    }
}
