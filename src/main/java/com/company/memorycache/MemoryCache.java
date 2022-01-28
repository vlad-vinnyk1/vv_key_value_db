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
        dumpOnDiskIfNeeded();
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

    public void purge() {
        synchronized (MemoryCache.class) {
            avlTree = Collections.synchronizedMap(new TreeMap<>());
        }
    }

    private void dumpOnDiskIfNeeded() {
        if (avlTree.size() >= props.SSTableDumpThreshold()) {
            synchronized (MemoryCache.class) {
                String filePath = ssTable.write(props.ssTablePath(), avlTree);
                log.info("AVL Tree has been dumped to: " + filePath);
                avlTree = Collections.synchronizedMap(new TreeMap<>());
            }
        }
    }
}
