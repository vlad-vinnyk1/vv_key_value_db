package com.company.sstable.csv;

import com.company.utils.Utils;
import io.vavr.Tuple;
import io.vavr.collection.List;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

import static com.company.utils.Utils.fromIteratorToStream;

// An Encapsulate some logic CSV files I.O.
@UtilityClass
public class CSVFileDao {

    @SneakyThrows
    public static String write(String ssTablesPath, Map<String, String> map) {
        String filePath = ssTablesPath + "/" + Utils.randomFileNameWithExtension(".csv");
        FileWriter output = new FileWriter(filePath);
        try (ICsvListWriter listWriter = new CsvListWriter(output, CsvPreference.STANDARD_PREFERENCE)) {
            map.forEach((key, value) -> writeWrappedException(listWriter, key, value));
        }
        return filePath;
    }

    @SneakyThrows
    public static TreeMap<String, String> read(String filePath) {
        return List.ofAll(fromIteratorToStream(new CsvFileIterator(filePath)))
                .map(dto -> Tuple.of(dto.getKey(), dto.getValue()))
                .foldLeft(emptyTreeMap(), io.vavr.collection.TreeMap::put)
                .toJavaMap();
    }

    private io.vavr.collection.TreeMap<String, String> emptyTreeMap() {
        return io.vavr.collection.TreeMap.empty();
    }

    @SneakyThrows
    public static String reWrite(String ssTablesPath, Map<String, String> map) {
        String tmpSsTablesPath = new File(ssTablesPath).getParent() + "/tmp";
        try {
            FileUtils.forceMkdir(new File(tmpSsTablesPath));
            String file = write(tmpSsTablesPath, map);
            Path filePath = Path.of(file);
            FileUtils.cleanDirectory(new File(ssTablesPath));
            String targetFilePath = ssTablesPath + "/" + Path.of(file).getFileName();
            Files.copy(filePath, Path.of(targetFilePath));
            return targetFilePath;
        } finally {
            FileUtils.cleanDirectory(new File(tmpSsTablesPath));
        }
    }

    @SneakyThrows
    private void writeWrappedException(ICsvListWriter listWriter, String key, String value) {
        listWriter.write(key, value);
    }
}
