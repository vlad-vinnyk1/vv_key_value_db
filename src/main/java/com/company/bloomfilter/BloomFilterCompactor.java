package com.company.bloomfilter;

import com.company.utils.Utils;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.spark.util.sketch.BloomFilter;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.commons.lang3.StringUtils.EMPTY;

@Log4j2
@Service
public class BloomFilterCompactor {
    public synchronized BloomFilter merge(String path, long bloomFilterExpectedElementsNumber) {
        BloomFilter mergedBloomFilter = Utils.listFiles(path)
                .map(Utils::toInputStream)
                .map(this::readFrom)
                .reduce(this::mergeInPlace)
                .orElse(BloomFilter.create(bloomFilterExpectedElementsNumber));

        return rewriteBloomFilterFolder(mergedBloomFilter, path);
    }

    @SneakyThrows
    private BloomFilter rewriteBloomFilterFolder(BloomFilter bloomFilter, String bloomFilterPath) {
        FileUtils.cleanDirectory(new File(bloomFilterPath));
        bloomFilter.writeTo(
                Files.newOutputStream(
                        Path.of(bloomFilterPath + "/" + Utils.randomFileNameWithExtension(EMPTY))
                )
        );

        return bloomFilter;
    }

    @SneakyThrows
    protected BloomFilter readFrom(InputStream is) {
        return BloomFilter.readFrom(is);
    }

    @SneakyThrows
    protected BloomFilter mergeInPlace(BloomFilter b1, BloomFilter b2) {
        return b1.mergeInPlace(b2);
    }
}
