package com.company;

import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

import static com.google.common.hash.Funnels.stringFunnel;

@Log4j2
@Service
public class BloomFilter {
    private com.google.common.hash.BloomFilter<String> bloomFilter = createBloomFilter();

    public void put(String key) {
        bloomFilter.put(key);
    }

    public boolean mightContain(String key) {
        return bloomFilter.mightContain(key);
    }

    public void purge() {
        bloomFilter = createBloomFilter();
    }

    private com.google.common.hash.BloomFilter<String> createBloomFilter() {
        return com.google.common.hash.BloomFilter.create(stringFunnel(StandardCharsets.UTF_8), 10000000);
    }
}
