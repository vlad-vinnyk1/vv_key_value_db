package com.company;

import com.company.properties.Properties;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class DatabaseIntegrationTest {
    @Autowired
    private Properties props;

    @Autowired
    private DatabaseOperations dbMainApi;

    @Autowired
    private BloomFilter bloomFilter;

    @Autowired
    private MemoryCache memoryCache;

    @Autowired
    private SSTableHelper ssTable;

    @SneakyThrows
    @AfterEach
    public void cleanUp() {
        ssTable.purge();
        bloomFilter.purge();
        memoryCache.purge();
    }

    @Test
    public void assertKeyAddedAndRetrievedFromMemory() {
        String key = "key1";
        String expected = "value1";

        dbMainApi.put(key, expected);
        Assertions.assertEquals(dbMainApi.get(key), expected);
    }

    @Test
    public void assertAddKeyUpdatedBloomAndMemoryCacheButNotDumpedToDiskBecauseOfThreshold() {
        String key = "key1";
        String expected = "value1";

        dbMainApi.put(key, expected);

        Assertions.assertAll(
                () -> Assertions.assertTrue(bloomFilter.mightContain(key)),
                () -> Assertions.assertEquals(memoryCache.get(key), expected),
                () -> Assertions.assertNull(ssTable.searchInLogFiles(key))
        );
    }

    @Test
    public void assertKeyAddedAndRetrievedFromDiskAndInMemory() {
        Tuple2<String, String> tup1 = Tuple.of("diskKey1", "diskValue1");
        Tuple2<String, String> tup2 = Tuple.of("diskKey2", "diskValue2");
        Tuple2<String, String> tup3 = Tuple.of("diskKey3", "diskValue3");
        Tuple2<String, String> tup4 = Tuple.of("memoryKey1", "memoryValue1");

        dbMainApi.put(tup1._1, tup1._2);
        dbMainApi.put(tup2._1, tup2._2);
        dbMainApi.put(tup3._1, tup3._2);
        dbMainApi.put(tup4._1, tup4._2);

        Assertions.assertAll(
                () -> Assertions.assertEquals(dbMainApi.get(tup1._1), tup1._2),
                () -> Assertions.assertEquals(dbMainApi.get(tup2._1), tup2._2),
                () -> Assertions.assertEquals(dbMainApi.get(tup3._1), tup3._2),
                () -> Assertions.assertEquals(dbMainApi.get(tup4._1), tup4._2)
        );
    }

    @Test
    public void assertKeyAddedAndDumpedToDiskAndPartInMemory() {
        Tuple2<String, String> tup1 = Tuple.of("diskKey1", "diskValue1");
        Tuple2<String, String> tup2 = Tuple.of("diskKey2", "diskValue2");
        Tuple2<String, String> tup3 = Tuple.of("diskKey3", "diskValue3");
        Tuple2<String, String> tup4 = Tuple.of("memoryKey1", "memoryValue1");

        dbMainApi.put(tup1._1, tup1._2);
        dbMainApi.put(tup2._1, tup2._2);
        dbMainApi.put(tup3._1, tup3._2);
        dbMainApi.put(tup4._1, tup4._2);

        Assertions.assertAll(
                () -> Assertions.assertEquals(memoryCache.size(), 1),
                () -> Assertions.assertEquals(memoryCache.get(tup4._1), tup4._2),
                () -> Assertions.assertNull(memoryCache.get(tup1._1)),
                () -> Assertions.assertEquals(ssTable.searchInLogFiles(tup1._1), tup1._2),
                () -> Assertions.assertEquals(ssTable.searchInLogFiles(tup2._1), tup2._2),
                () -> Assertions.assertEquals(ssTable.searchInLogFiles(tup3._1), tup3._2),
                () -> Assertions.assertNull(ssTable.searchInLogFiles(tup4._1))
        );
    }

    @Test
    public void assertUpdateInMemory() {
        dbMainApi.put("memoryKey1", "memoryValue1");
        dbMainApi.update("memoryKey1", "memoryValue2Updated");

        Assertions.assertAll(
                () -> Assertions.assertEquals(dbMainApi.get("memoryKey1"), "memoryValue2Updated"),
                () -> Assertions.assertNull(ssTable.searchInLogFiles("memoryKey1"))
        );
    }

    @Test
    public void assertUpdateOnDisk() {
        Tuple2<String, String> tup1 = Tuple.of("diskKey1", "diskValue1");
        Tuple2<String, String> tup2 = Tuple.of("diskKey2", "diskValue2");
        Tuple2<String, String> tup3 = Tuple.of("diskKey3", "diskValue3");
        Tuple2<String, String> tup4 = Tuple.of("memoryKey1", "memoryValue1");

        dbMainApi.put(tup1._1, tup1._2);
        dbMainApi.put(tup2._1, tup2._2);
        dbMainApi.put(tup3._1, tup3._2);
        dbMainApi.put(tup4._1, tup4._2);

        dbMainApi.update(tup1._1, "updatedValue1");
        dbMainApi.update(tup2._1, "updatedValue2");
        dbMainApi.update(tup3._1, "updatedValue3");
        dbMainApi.update(tup4._1, "updatedValue4");

        Assertions.assertAll(
                () -> Assertions.assertEquals(dbMainApi.get(tup1._1), "updatedValue1"),
                () -> Assertions.assertEquals(dbMainApi.get(tup2._1), "updatedValue2"),
                () -> Assertions.assertEquals(dbMainApi.get(tup3._1), "updatedValue3"),
                () -> Assertions.assertEquals(dbMainApi.get(tup4._1), "updatedValue4")
        );

        Assertions.assertAll(
                () -> Assertions.assertEquals(ssTable.searchInLogFiles(tup1._1), "updatedValue1"),
                () -> Assertions.assertEquals(ssTable.searchInLogFiles(tup2._1), "updatedValue2"),
                () -> Assertions.assertEquals(ssTable.searchInLogFiles(tup3._1), "diskValue3"),
                () -> Assertions.assertEquals(memoryCache.get(tup3._1), "updatedValue3"),
                () -> Assertions.assertEquals(memoryCache.get(tup4._1), "updatedValue4")
        );
    }

    @Test
    public void assertRemoveInMemory() {
        dbMainApi.put("memoryKey1", "memoryValue1");
        dbMainApi.remove("memoryKey1");

        Assertions.assertAll(
                () -> Assertions.assertNull(dbMainApi.get("memoryKey1"))
        );
    }

    @Test
    public void assertRemoveFromDiskAndInMemory() {
        Tuple2<String, String> tup1 = Tuple.of("diskKey1", "diskValue1");
        Tuple2<String, String> tup2 = Tuple.of("diskKey2", "diskValue2");
        Tuple2<String, String> tup3 = Tuple.of("diskKey3", "diskValue3");
        Tuple2<String, String> tup4 = Tuple.of("memoryKey1", "memoryValue1");

        dbMainApi.put(tup1._1, tup1._2);
        dbMainApi.put(tup2._1, tup2._2);
        dbMainApi.put(tup3._1, tup3._2);
        dbMainApi.put(tup4._1, tup4._2);

        dbMainApi.remove(tup1._1);
        dbMainApi.remove(tup2._1);
        dbMainApi.remove(tup3._1);
        dbMainApi.remove(tup4._1);

        Assertions.assertAll(
                () -> Assertions.assertNull(dbMainApi.get(tup1._1)),
                () -> Assertions.assertNull(dbMainApi.get(tup2._1)),
                () -> Assertions.assertNull(dbMainApi.get(tup3._1)),
                () -> Assertions.assertNull(dbMainApi.get(tup4._1))
        );
    }
}
