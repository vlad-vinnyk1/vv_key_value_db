package com.company.bloomfilter;

import com.company.config.PropertiesService;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.spark.util.sketch.BloomFilter;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.springframework.stereotype.Service;

import java.io.File;

@Log4j2
@Service
public class BloomFilterManager {

    private BloomFilter bloomFilter;

    private final PropertiesService propertiesService;

    public BloomFilterManager(PropertiesService propertiesService, BloomFilterCompactor compactor) {
        this.propertiesService = propertiesService;

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

    // For tests
    @SneakyThrows
    public synchronized void purge() {
        bloomFilter = BloomFilter.create(propertiesService.bloomFilterExpectedElementsNumber());
        FileUtils.cleanDirectory(new File(propertiesService.bloomFilterPath()));
    }
}
