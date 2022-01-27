package com.company.config;

import com.company.utils.Utils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class PropertiesService {
    @Value("${root.path}")
    private String rootPath;

    @Value("${sstable.disk.dump.threshold}")
    private int SSTableDumpThreshold;

    @Value("${sstable.compactor.file.number.threshold}")
    private long compactCounter;

    @Value("${bloom.filter.expectedElementsNumber}")
    private long bloomFilterExpectedElementsNumber;

    public String ssTablePath() {
        return Utils.createDirs(rootPath + "/sstable");
    }

    public long bloomFilterExpectedElementsNumber() {
        return bloomFilterExpectedElementsNumber;
    }

    public String bloomFilterPath() {
        return Utils.createDirs(rootPath + "/bloom_filter");
    }

    public int SSTableDumpThreshold() {
        return SSTableDumpThreshold;
    }

    public long compactCounter() {
        return compactCounter;
    }

}
