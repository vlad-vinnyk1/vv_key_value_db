package com.company.sstable.csv;

import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Builder
@Data
@RequiredArgsConstructor
public class CsvFileRecord {
    final String key;

    final String value;

    final String fileName;

    final CsvFileIterator iteratorReference;
}

