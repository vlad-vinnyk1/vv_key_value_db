package com.company.sstable.csv;

import lombok.SneakyThrows;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.FileReader;
import java.io.Reader;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class CsvFileIterator implements Iterator<CsvDto> {
    private final CsvListReader csvReader;
    private final String filePath;
    private List<String> next = null;

    @SneakyThrows
    public CsvFileIterator(String filePath) {
        this.filePath = filePath;
        Reader reader = new FileReader(filePath);
        csvReader = new CsvListReader(reader, CsvPreference.STANDARD_PREFERENCE);
    }

    @SneakyThrows
    @Override
    public synchronized boolean hasNext() {
        next = csvReader.read();
        return next != null;
    }

    @Override
    public synchronized CsvDto next() {
        Objects.requireNonNull(next, "Next is null, consider call hasNext before");
        return CsvDto.builder()
                .key(next.get(0))
                .value(next.get(1))
                .fileName(filePath)
                .iteratorReference(this)
                .build();
    }
}
