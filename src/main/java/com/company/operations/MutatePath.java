package com.company.operations;

import com.company.MemoryCache;
import com.company.BloomFilter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

@Log4j2
@RequiredArgsConstructor
@Service
public class MutatePath {
    private final MemoryCache memoryCache;
    private final BloomFilter presenceBloomFilter;

    public void put(String key, String value) {
        try {
            memoryCache.put(key, value);
            presenceBloomFilter.put(key);
        } catch (Exception e) {
            log.error("Can't add to key to database: {}.", key);
            memoryCache.remove(key);
        }
    }

    public void remove(String key) {
        memoryCache.remove(key);
    }
}
