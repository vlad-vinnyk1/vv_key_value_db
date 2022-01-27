package com.company.utils;

import io.vavr.control.Try;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;

@Log4j2
@UtilityClass
public class Utils {
    public String createDirs(String path) {
        if (new File(path).mkdirs()) {
            log.info("File '{}' has been created", path);
        }

        return path;
    }

    public String randomFileNameWithExtension(String ext) {
        long epochSecond = Instant.now().getEpochSecond();
        long nano = Instant.now().getNano();
        String randomPart = randomAlphabetic(8);

        return epochSecond + "_" + nano + "_" + randomPart + ext;
    }

    public Stream<Path> listFiles(String ssTablePath) {
        Path path = Path.of(ssTablePath);
        return Try.ofSupplier(() -> listFiles(path)).getOrElse(Stream.empty());
    }

    public static List<String> listFilesInSortedOrder(String path) {
        return Utils.listFiles(path)
                .map(Path::toAbsolutePath)
                .map(Path::toString)
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());
    }

    @SneakyThrows
    private Stream<Path> listFiles(Path path) {
        return Files.list(path);
    }

    public static class Constants {
        public static final String TOMB = "@@_[TOBM]_@@";
    }
}
