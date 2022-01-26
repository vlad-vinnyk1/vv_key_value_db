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
    private Map<String, String> inMemAVLTree = Collections.synchronizedMap(new TreeMap<>());
    private final SSTableManager ssTable;
    private final PropertiesService props;

    public String get(String key) {
        return inMemAVLTree.get(key);
    }

    public void put(String key, String value) {
        inMemAVLTree.put(key, value);
        dumpOnDiskIfNeeded();
    }

    public int size() {
        return inMemAVLTree.size();
    }

    public void remove(String key) {
        inMemAVLTree.put(key, TOMB);
    }

    public void purge() {
        inMemAVLTree = Collections.synchronizedMap(new TreeMap<>());
    }

    private void dumpOnDiskIfNeeded() {
        if (inMemAVLTree.size() >= props.SSTableDumpThreshold()) {
            synchronized (MemoryCache.class) {
                String filePath = ssTable.dumpMapToDisk(props.ssTablePath(), inMemAVLTree);
                log.info("AVL Tree has been dumped to: " + filePath);
                inMemAVLTree = Collections.synchronizedMap(new TreeMap<>());
            }
        }
    }
}
