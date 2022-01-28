package com.company.memorycache;

import com.company.config.PropertiesService;
import com.company.sstable.SSTableManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import static com.company.utils.Utils.Constants.TOMB;

@RequiredArgsConstructor
@Log4j2
@Service
public class MemoryCache {
    private Map<String, String> avlTree = Collections.synchronizedMap(new TreeMap<>());
    private final SSTableManager ssTable;
    private final PropertiesService props;

    public String get(String key) {
        return avlTree.get(key);
    }

    public void put(String key, String value) {
        avlTree.put(key, value);
        dumpOnDisk();
    }

    public boolean contains(String key) {
        return avlTree.containsKey(key);
    }

    public int size() {
        return avlTree.size();
    }

    public void remove(String key) {
        avlTree.put(key, TOMB);
    }

    // For tests
    public synchronized void purge() {
        avlTree = Collections.synchronizedMap(new TreeMap<>());
    }

    private void dumpOnDisk() {
        synchronized (MemoryCache.class) {
            if (avlTree.size() >= props.SSTableDumpThreshold()) {
                String filePath = ssTable.write(props.ssTablePath(), avlTree);
                log.info("AVL Tree has been dumped to: " + filePath);
                avlTree = Collections.synchronizedMap(new TreeMap<>());
            }
        }
    }
}
