package com.company.sstable;

import com.company.sstable.csv.CSVFileDao;
import com.company.sstable.csv.CsvDto;
import com.company.sstable.csv.CsvFileIterator;
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
        PriorityQueue<CsvDto> heap = initHeap(iterators);
        while (!heap.isEmpty()) {
            CsvDto current = heap.poll();
            if (isNotTomb(current.getValue())) {
                inMemAVLTree.put(current.getKey(), current.getValue());
            }
            putNextElementToHeap(heap, current);
            skipOutdatedElements(heap, current);
        }

        return CSVFileDao.reWrite(path, inMemAVLTree);
    }

    private void skipOutdatedElements(PriorityQueue<CsvDto> heap, CsvDto current) {
        while (heap.peek() != null && StringUtils.equals(heap.peek().getKey(), current.getKey())) {
            putNextElementToHeap(heap, heap.poll());
        }
    }

    private void putNextElementToHeap(PriorityQueue<CsvDto> heap, CsvDto element) {
        if (element.getIteratorReference().hasNext()) {
            heap.add(element.getIteratorReference().next());
        }
    }

    private PriorityQueue<CsvDto> initHeap(List<CsvFileIterator> iterators) {
        PriorityQueue<CsvDto> res = new PriorityQueue<>(getCsvDtoComparator());
        iterators.stream()
                .map(a -> a.hasNext() ? a.next() : null)
                .filter(Objects::nonNull)
                .forEach(res::add);

        return res;
    }

    private Comparator<CsvDto> getCsvDtoComparator() {
        return Comparator.comparing(CsvDto::getKey).thenComparing(Comparator.comparing(CsvDto::getFileName).reversed());
    }
}
