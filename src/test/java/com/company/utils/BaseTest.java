package com.company.utils;

import com.company.DatabaseNode;
import com.company.bloomfilter.BloomFilterManager;
import com.company.config.PropertiesService;
import com.company.memorycache.MemoryCache;
import com.company.sstable.SSTableManager;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;

public class BaseTest {
    @Autowired
    protected PropertiesService props;

    @Autowired
    protected DatabaseNode node;

    @Autowired
    protected BloomFilterManager bloomFilter;

    @Autowired
    protected MemoryCache memoryCache;

    @Autowired
    protected SSTableManager ssTable;

    @SneakyThrows
    @AfterEach
    public void cleanUp() {
        ssTable.purge();
        bloomFilter.purge();
        memoryCache.purge();
    }
}
