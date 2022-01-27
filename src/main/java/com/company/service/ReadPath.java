package com.company.service;

import com.company.bloomfilter.BloomFilterManager;
import com.company.memorycache.MemoryCache;
import com.company.sstable.SSTableManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import static com.company.utils.Utils.Constants.TOMB;

@Log4j2
@RequiredArgsConstructor
@Service
public class ReadPath {
    private final BloomFilterManager bloomFilter;
    private final MemoryCache memoryCache;
    private final SSTableManager ssTableManager;

    public String get(String key) {
        return bloomFilter.mightContain(key) ? search(key) : null;
    }

    private String search(String key) {
        String value = isInMemoryCache(key) ? memoryCache.get(key) : ssTableManager.searchInLogFiles(key);
        return isNotTomb(value) ? value : null;
    }

    private boolean isInMemoryCache(String key) {
        return memoryCache.get(key) != null;
    }

    private boolean isNotTomb(String value) {
        return !TOMB.equals(value);
    }
}
