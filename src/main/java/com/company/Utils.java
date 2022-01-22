package com.company;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

@Log4j2
@UtilityClass
public class Utils {
    public void createDirs(String path) {
        if (new File(path).mkdirs()) {
            log.info("File '{}' has been created", path);
        }
    }

    @SneakyThrows
    public Stream<Path> list(String path) {
        return Files.list(Path.of(path));
    }

    public static class Constants {
        public static final String TOMB = "@@_[TOBM]_@@";
    }
}
