package com.company.sstable;

import com.company.config.PropertiesService;
import com.company.memorycache.MemoryCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class SSTableCompactorTests {
    @Autowired
    private MemoryCache memoryCache;

    @Autowired
    private PropertiesService pros;

    @Autowired
    private SSTableCompactor SSTableCompactor;

    @Test
    public void testCompaction() {
        // First File
        memoryCache.put("a", "[a]value");
        memoryCache.put("b", "[b]value");
        memoryCache.put("c", "[c]value");

        // Second File
        memoryCache.put("a2", "[a3]value");
        memoryCache.put("b2", "[b3]value");
        memoryCache.put("c2", "[c3]value");

        //Remove Some
        memoryCache.remove("a");
        memoryCache.remove("a2");
        memoryCache.put("qq", "[qq]value");

        SSTableCompactor.compact(pros.ssTablePath());
    }
}
