package com.company.sstable;

import com.company.config.PropertiesService;
import com.company.sstable.csv.CSVFileDao;
import com.company.utils.Utils;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

@RequiredArgsConstructor
@Log4j2
@Service
public class SSTableManager {
    private final SSTableCompactor compactor;

    private final PropertiesService props;

    private final AtomicInteger filesWritten = new AtomicInteger(0);

    @SneakyThrows
    public String write(String ssTablesPath, Map<String, String> map) {
        String filePath = CSVFileDao.write(ssTablesPath, map);
        filesWritten.incrementAndGet();
        if (isTimeToCompact()) {
            filesWritten.set(0);
            return compactor.compact(ssTablesPath);
        } else {
            return filePath;
        }
    }

    @SneakyThrows
    // TODO Binary search. Need to think about some algo and Data Structure to perform Binary Search on disk, without loading all in memory???
    public String get(String key) {
        return Utils.listFilesInSortedOrder(props.ssTablePath()).stream()
                .map(CSVFileDao::read)
                .map(logMap -> logMap.get(key))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    // For tests only.
    @SneakyThrows
    public synchronized void purge() {
        FileUtils.deleteDirectory(new File(props.ssTablePath()));
    }

    private boolean isTimeToCompact() {
        return filesWritten.get() == props.compactCounter();
    }
}
