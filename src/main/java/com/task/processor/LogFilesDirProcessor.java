package com.task.processor;

import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class LogFilesDirProcessor<T> {

    private final Function<String, ? extends T> fromLineMapper;
    private final Function<? super T, String> toLineMapper;

    public long process(String logFilesDirPath, String resultFilePath, Predicate<T> filter, Comparator<T> comparator) throws IOException {
        AtomicLong counter = new AtomicLong(0);
        try (Stream<String> pipeline =
                     configurePipeline(Paths.get(logFilesDirPath), filter, comparator)
                             .peek(item -> counter.getAndIncrement())) {
            Files.write(Paths.get(resultFilePath), (Iterable<String>) pipeline::iterator);
            return counter.get();
        }
    }

    private Stream<String> configurePipeline(Path logFilesDirPath, Predicate<T> filter, Comparator<T> comparator) throws IOException {
        try (Stream<Path> filesStream = Files.list(logFilesDirPath)) {
            return filesStream
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toCollection(LinkedList::new))
                    .stream()
                    .flatMap(LogFilesDirProcessor::getFileLinesStreamUnchecked)
                    .map(fromLineMapper)
                    .filter(Objects::nonNull)
                    .filter(filter)
                    .sorted(comparator)
                    .map(toLineMapper)
                    .parallel();
        }
    }

    private static Stream<String> getFileLinesStreamUnchecked(Path path) {
        try {
            return Files.lines(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
