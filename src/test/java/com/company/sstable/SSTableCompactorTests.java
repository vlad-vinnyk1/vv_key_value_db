package com.company.sstable;

import com.company.BaseTest;
import com.company.sstable.csv.CSVFileDao;
import com.company.utils.Utils;
import lombok.SneakyThrows;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.File;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class SSTableCompactorTests extends BaseTest {

    @Autowired
    private SSTableCompactor compactor;

    @SneakyThrows
    @AfterEach
    public void cleanUp() {
        ssTable.purge();
        bloomFilter.purge();
        memoryCache.purge();
    }

    @Test
    public void testCompactionCompactedAsExpected() {
        // First File
        memoryCache.put("a", "[a]value");
        memoryCache.put("b", "[b]value");
        memoryCache.put("c", "[c]value");

        // Second File
        memoryCache.put("a2", "[a3]value");
        memoryCache.put("b2", "[b3]value");
        memoryCache.put("c2", "[c3]value");

        //Remove/Add Some as third file
        memoryCache.remove("a");
        memoryCache.remove("a2");
        memoryCache.put("qq", "[qq]value");

        // Updates
        memoryCache.put("b", "new_b");
        memoryCache.put("c", "new_c");
        memoryCache.put("b2", "new_b2");

        Assertions.assertEquals(4, Utils.listFilesInSortedOrder(props.ssTablePath()).size());
        compactor.compact(props.ssTablePath());

        TreeMap<String, String> read = CSVFileDao.read(Utils.listFilesInSortedOrder(props.ssTablePath()).get(0));
        Assertions.assertAll(
                () -> assertEquals(Utils.listFilesInSortedOrder(props.ssTablePath()).size(), 1),
                () -> Assertions.assertNull(read.get("a"), "The keys supposed to be removed"),
                () -> Assertions.assertEquals(read.get("b"), "new_b"),
                () -> Assertions.assertEquals(read.get("c"), "new_c"),
                () -> Assertions.assertNull(read.get("a2"), "The keys supposed to be removed"),
                () -> Assertions.assertEquals(read.get("b2"), "new_b2"),
                () -> Assertions.assertEquals(read.get("c2"), "[c3]value"),
                () -> Assertions.assertEquals(read.get("qq"), "[qq]value")
        );
    }

    @SneakyThrows
    @Test
    public void testCompactorNotThrowingException() {
        FileUtils.cleanDirectory(new File(props.ssTablePath()));
        compactor.compact(props.ssTablePath());
    }

    @SneakyThrows
    @Test
    public void testCompactedWhenFileNumberThresholdExceeded() {
        // First File
        memoryCache.put("a", "[a]value");
        memoryCache.put("b", "[b]value");
        memoryCache.put("c", "[c]value");

        // Second File
        memoryCache.put("a2", "[a2]value");
        memoryCache.put("b2", "[b2]value");
        memoryCache.put("c2", "[c2]value");

        // Third File
        memoryCache.put("a3", "[a3]value");
        memoryCache.put("b3", "[b3]value");
        memoryCache.put("c3", "[c3]value");

        // Forth File
        memoryCache.put("a4", "[a4]value");
        memoryCache.put("b4", "[b4]value");
        memoryCache.put("c4", "[c4]value");

        // Fives File
        memoryCache.put("a5", "[a5]value");
        memoryCache.put("b5", "[b5]value");
        memoryCache.put("c5", "[c5]value");
        Assertions.assertEquals(5, Utils.listFilesInSortedOrder(props.ssTablePath()).size());

        // Sixth File
        memoryCache.put("a6", "[a6]value");
        memoryCache.put("b6", "[b6]value");
        memoryCache.put("c6", "[c6]value");

        Assertions.assertEquals(1, Utils.listFilesInSortedOrder(props.ssTablePath()).size());
    }
}
