package com.company.service;

import com.company.memorycache.MemoryCache;
import com.company.bloomfilter.BloomFilterManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

@Log4j2
@RequiredArgsConstructor
@Service
public class UpdatePath {
    private final MemoryCache memoryCache;
    private final BloomFilterManager bloomFilter;

    public void put(String key, String value) {
        try {
            memoryCache.put(key, value);
            bloomFilter.put(key);
        } catch (Exception e) {
            log.error("Can't add to key to database: {}.", key);
            memoryCache.remove(key);
        }
    }

    public void remove(String key) {
        memoryCache.remove(key);
    }
}
