package com.company.bloomfilter;

import com.company.config.PropertiesService;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.spark.util.sketch.BloomFilter;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class BloomFilterManager {

    private BloomFilter bloomFilter;

    private final BloomFilterCompactor compactor;

    private final PropertiesService propertiesService;

    public BloomFilterManager(PropertiesService propertiesService, BloomFilterCompactor compactor) {
        this.propertiesService = propertiesService;
        this.compactor = compactor;

        bloomFilter = compactor.merge(
                propertiesService.bloomFilterPath(),
                propertiesService.bloomFilterExpectedElementsNumber()
        );
    }

    public void put(String key) {
        bloomFilter.put(key);
    }

    public boolean mightContain(String key) {
        return bloomFilter.mightContain(key);
    }

    @SneakyThrows
    public void purge() {
        synchronized (BloomFilterManager.class) {
            bloomFilter = BloomFilter.create(propertiesService.bloomFilterExpectedElementsNumber());
            compactor.rewriteBloomFilter(bloomFilter, propertiesService.bloomFilterPath());
        }
    }
}
