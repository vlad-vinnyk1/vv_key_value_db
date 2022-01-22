package com.company.operations;

import com.company.BloomFilter;
import com.company.MemoryCache;
import com.company.SSTableHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import static com.company.Utils.Constants.TOMB;

@Log4j2
@RequiredArgsConstructor
@Service
public class ReadPath {
    private final BloomFilter bloomFilter;
    private final MemoryCache memoryCache;
    private final SSTableHelper ssTableManager;

    public String get(String key) {
        return bloomFilter.mightContain(key) ? search(key) : null;
    }

    private String search(String key) {
        String value = memoryCache.get(key) != null ? memoryCache.get(key) : ssTableManager.searchInLogFiles(key);
        return !TOMB.equals(value) ? value : null;
    }
}
