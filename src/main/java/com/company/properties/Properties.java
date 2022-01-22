package com.company.properties;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

//@RequiredArgsConstructor
@Service
public class Properties {
    @Value("${db.rootpath}")
    private String rootPath;

    @Value("${db.sstable.disk.dump.threshold}")
    private int sstableDumpThreshold;

    public String ssTablePath() {
        return rootPath + "/sstable";
    }

    public int sstableDumpThreshold() {
        return sstableDumpThreshold;
    }
}
