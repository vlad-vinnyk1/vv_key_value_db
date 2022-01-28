package com.company.utils;

import com.company.sstable.csv.CsvFileRecord;
import com.company.sstable.csv.CsvFileIterator;
import io.vavr.control.Try;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.company.utils.Utils.Constants.TOMB;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;

@Log4j2
@UtilityClass
public class Utils {
    public static String createDirs(String path) {
        if (new File(path).mkdirs()) {
            log.info("File '{}' has been created", path);
        }

        return path;
    }

    public static String randomFileNameWithExtension(String ext) {
        long epochSecond = Instant.now().getEpochSecond();
        long nano = Instant.now().getNano();
        String randomPart = randomAlphabetic(8);

        return epochSecond + "_" + nano + "_" + randomPart + ext;
    }

    public static Stream<Path> listFiles(String ssTablePath) {
        Path path = Path.of(ssTablePath);
        return Try.ofSupplier(() -> listFiles(path)).getOrElse(Stream.empty());
    }

    public static Stream<CsvFileRecord> fromIteratorToStream(CsvFileIterator csvFileIterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(csvFileIterator, Spliterator.ORDERED), false);
    }

    public static List<String> listFilesInSortedOrder(String path) {
        return Utils.listFiles(path)
                .map(Path::toAbsolutePath)
                .map(Path::toString)
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());
    }

    // The workaround of the Java API does not allow doing stuff in a functional way.
    public  <T> PriorityQueue<T> addInFunctionalWay(PriorityQueue<T> q, T element) {
        q.add(element);
        return q;
    }

    // The workaround of the Java API to wrap java checked exceptions.
    @SneakyThrows
    public InputStream toInputStream(Path path) {
        return Files.newInputStream(path);
    }

    @SneakyThrows
    private Stream<Path> listFiles(Path path) {
        return Files.list(path);
    }

    public static boolean isTomb(String str) {
        return TOMB.equals(str);
    }

    public static boolean isNotTomb(String str) {
        return !isTomb(str);
    }

    public static class Constants {
        public static final String TOMB = "@@_[TOBM]_@@";
    }
}
