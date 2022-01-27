package com.company.sstable;

import com.company.utils.Utils;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@UtilityClass
public class CSVFileManager {

    @SneakyThrows
    static String write(String ssTablesPath, Map<String, String> map) {
        String filePath = ssTablesPath + "/" + Utils.randomFileNameWithExtension(".csv");
        FileWriter output = new FileWriter(filePath);
        try (ICsvListWriter listWriter = new CsvListWriter(output, CsvPreference.STANDARD_PREFERENCE)) {
            map.forEach((key, value) -> writeWrappedException(listWriter, key, value));
        }
        return filePath;
    }

    @SneakyThrows
    static TreeMap<String, String> read(String filePath) {
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

    @SneakyThrows
    private void writeWrappedException(ICsvListWriter listWriter, String key, String value) {
        listWriter.write(key, value);
    }
}
