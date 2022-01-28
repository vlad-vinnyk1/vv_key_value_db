package com.company.sstable;

import com.company.sstable.csv.CSVFileDao;
import com.company.sstable.csv.CsvDto;
import com.company.sstable.csv.CsvFileIterator;
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
        List<CsvFileIterator> iterators = listFiles(path)
                .map(Path::toString)
                .map(CsvFileIterator::new)
                .collect(Collectors.toList());

        Map<String, String> inMemAVLTree = Collections.synchronizedMap(new TreeMap<>());

        // Classic algo: Merge Files lazily through iterator + PriorityQueue to speed up searching.
        // Initializing the heap by first elements of each file.
        PriorityQueue<CsvDto> minHeap = initMinHeap(iterators);

        while (!minHeap.isEmpty()) {
            CsvDto currElement = minHeap.poll();
            if (isNotTomb(currElement.getValue())) {
                inMemAVLTree.put(currElement.getKey(), currElement.getValue());
            }
            // As corresponding to the iterator element was polled, we need to add the next one.
            addNextElementToHeapFromIterator(minHeap, currElement);
            // If there are keys in the heap that are equal to currentElement.key, they are considered outdated and should be removed.
            removeAllKeysFromHeap(currElement.getKey(), minHeap);
        }

        return CSVFileDao.reWrite(path, inMemAVLTree);
    }

    private void removeAllKeysFromHeap(String keyToRemove, PriorityQueue<CsvDto> heap) {
        while (heap.peek() != null && StringUtils.equals(heap.peek().getKey(), keyToRemove)) {
            addNextElementToHeapFromIterator(heap, heap.poll());
        }
    }

    private void addNextElementToHeapFromIterator(PriorityQueue<CsvDto> heap, CsvDto element) {
        if (element.getIteratorReference().hasNext()) {
            heap.add(element.getIteratorReference().next());
        }
    }

    private PriorityQueue<CsvDto> initMinHeap(List<CsvFileIterator> iterators) {
        return io.vavr.collection.List.ofAll(iterators.stream())
                .map(a -> a.hasNext() ? a.next() : null)
                .filter(Objects::nonNull)
                .foldLeft(new PriorityQueue<>(comparator()), Utils::addInFunctionalWay);
    }

    private Comparator<CsvDto> comparator() {
        return Comparator.comparing(CsvDto::getKey).thenComparing(Comparator.comparing(CsvDto::getFileName).reversed());
    }
}
