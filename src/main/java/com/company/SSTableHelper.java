package com.company;

import com.company.properties.Properties;
import io.vavr.control.Try;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.springframework.stereotype.Service;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.*;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;

@RequiredArgsConstructor
@Log4j2
@Service
public class SSTableHelper {
    private final Properties props;

    @SneakyThrows
    public String dumpMapToDisk(String ssTablesPath, Map<String, String> map) {
        Utils.createDirs(ssTablesPath);
        String filePath = ssTablesPath + "/" + fileName();
        FileWriter output = new FileWriter(filePath);
        try (ICsvListWriter listWriter = new CsvListWriter(output, CsvPreference.STANDARD_PREFERENCE)) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                listWriter.write(entry.getKey(), entry.getValue());
            }
        }

        return filePath;
    }

    @SneakyThrows
    public String searchInLogFiles(String key) {
        String ssTablePath = props.ssTablePath();
        List<String> sortedLogFiles = listFiles(ssTablePath)
                .map(Path::toAbsolutePath)
                .map(Path::toString)
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());

        return searchInLogFiles(key, sortedLogFiles);
    }

    @SneakyThrows
    private Stream<Path> listFiles(String ssTablePath) throws IOException {
        return Try.ofSupplier(() -> Utils.list(ssTablePath)).getOrElse(Stream.empty());
    }

    @SneakyThrows
    public void purge() {
        FileUtils.deleteDirectory(new File(props.ssTablePath()));
    }

    private String searchInLogFiles(String key, List<String> sortedFiles) {
        //TODO Binary search on disk more efficient than read all file in memory;
        for (String file : sortedFiles) {
            log.info("Searching in file {}", file);
            Map<String, String> map = readMap(file);
            if (map.containsKey(key)) {
                log.info("Key {} is Found in file {}", key, file);
                return map.get(key);
            }
        }

        return null;
    }

    @SneakyThrows
    private TreeMap<String, String> readMap(String filePath) {
        TreeMap<String, String> res = new TreeMap<>();
        Reader reader = new FileReader(filePath);
        CsvListReader csvReader = new CsvListReader(reader, CsvPreference.STANDARD_PREFERENCE);
        List<String> read = csvReader.read();
        while (read != null) {
            res.put(read.get(0), read.get(1));
            read = csvReader.read();
        }

        return res;
    }

    private String fileName() {
        long epochSecond = Instant.now().getEpochSecond();
        long nano = Instant.now().getNano();
        String randomPart = randomAlphabetic(8);

        return epochSecond + "_" + nano + "_" + randomPart + ".csv";
    }
}
