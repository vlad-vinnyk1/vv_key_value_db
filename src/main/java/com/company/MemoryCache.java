package com.company;

import com.company.properties.Properties;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import static com.company.Utils.Constants.TOMB;

@RequiredArgsConstructor
@Log4j2
@Service
public class MemoryCache {
    private Map<String, String> inMemoryAVLTree = Collections.synchronizedMap(new TreeMap<>());
    private final SSTableHelper ssTableManager;
    private final Properties props;

    public String get(String key) {
        return inMemoryAVLTree.get(key);
    }

    public void put(String key, String value) {
        inMemoryAVLTree.put(key, value);
        dumpOnDiskIfNeeded();
    }

    public int size() {
        return inMemoryAVLTree.size();
    }

    public void remove(String key) {
        inMemoryAVLTree.put(key, TOMB);
    }

    public void purge() {
        inMemoryAVLTree = Collections.synchronizedMap(new TreeMap<>());
    }

    private void dumpOnDiskIfNeeded() {
        if (inMemoryAVLTree.size() >= props.sstableDumpThreshold()) {
            synchronized (MemoryCache.class) {
                String filePath = ssTableManager.dumpMapToDisk(props.ssTablePath(), inMemoryAVLTree);
                log.info("AVL Tree has been dumped to: " + filePath);
                inMemoryAVLTree = Collections.synchronizedMap(new TreeMap<>());
            }
        }
    }
}
