package com.company.sstable;

import com.company.sstable.csv.CSVFileDao;
import com.company.sstable.csv.CsvFileIterator;
import com.company.sstable.csv.CsvFileRecord;
import com.company.utils.Utils;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static com.company.utils.Utils.isNotTomb;
import static com.company.utils.Utils.listFiles;

@Log4j2
@Service
public class SSTableCompactor {

    public String compact(String path) {
        List<CsvFileIterator> csvFilesIterators = listFiles(path)
                .map(Path::toString)
                .map(CsvFileIterator::new)
                .collect(Collectors.toList());

        Map<String, String> inMemAVLTree = Collections.synchronizedMap(new TreeMap<>());

        // Classic algo: Merge Files lazily through iterator + PriorityQueue to speed up searching.
        // Initializing the heap by first elements of each file.
        PriorityQueue<CsvFileRecord> recordsHeap = initMinHeap(csvFilesIterators);

        while (!recordsHeap.isEmpty()) {
            CsvFileRecord currentRecord = recordsHeap.poll();
            if (isNotTomb(currentRecord.getValue())) {
                inMemAVLTree.put(currentRecord.getKey(), currentRecord.getValue());
            }
            // As corresponding to the iterator element was polled, we need to add the next one.
            addNextRecordToHeapFromFileIterator(recordsHeap, currentRecord);
            // If there are keys in the heap that are equal to currentElement.key, they are considered outdated and should be removed.
            removeAllKeysFromHeap(currentRecord.getKey(), recordsHeap);
        }

        return CSVFileDao.reWrite(path, inMemAVLTree);
    }

    private void removeAllKeysFromHeap(String keyToRemove, PriorityQueue<CsvFileRecord> heap) {
        while (heap.peek() != null && StringUtils.equals(heap.peek().getKey(), keyToRemove)) {
            addNextRecordToHeapFromFileIterator(heap, heap.poll());
        }
    }

    private void addNextRecordToHeapFromFileIterator(PriorityQueue<CsvFileRecord> heap, CsvFileRecord element) {
        if (element.getIteratorReference().hasNext()) {
            heap.add(element.getIteratorReference().next());
        }
    }

    private PriorityQueue<CsvFileRecord> initMinHeap(List<CsvFileIterator> iterators) {
        return io.vavr.collection.List.ofAll(iterators.stream())
                .map(a -> a.hasNext() ? a.next() : null)
                .filter(Objects::nonNull)
                .foldLeft(new PriorityQueue<>(comparator()), Utils::addInFunctionalWay);
    }

    // Compare by key, then by file name in reverse order.
    // The file name is sortable and determines when the key was added. The newest one should rise on top of the heap.
    private Comparator<CsvFileRecord> comparator() {
        return Comparator.comparing(CsvFileRecord::getKey).thenComparing(Comparator.comparing(CsvFileRecord::getFileName).reversed());
    }
}
