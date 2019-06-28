package com.task.processor;

import com.task.util.FilesLinesStreamProvider;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class LogFilesDirProcessor<T> {

    @NonNull
    private final Function<String, ? extends T> fromLineMapper;
    @NonNull
    private final Function<? super T, String> toLineMapper;
    @NonNull
    private final FilesLinesStreamProvider filesLinesStreamProvider;

    public long process(String inputDirPath, String outputFilePath, Predicate<T> filter, Comparator<T> comparator) throws IOException {
        LongAdder counter = new LongAdder();
        try (Stream<String> pipeline =
                     configurePipeline(Paths.get(inputDirPath), filter, comparator)
                             .peek(item -> counter.increment())) {
            Files.write(Paths.get(outputFilePath), (Iterable<String>) pipeline::iterator);
            return counter.sum();
        }
    }

    private Stream<String> configurePipeline(Path inputDirPath, Predicate<T> filter, Comparator<T> comparator) throws IOException {
        try (Stream<Path> filesStream = Files.list(inputDirPath)) {
            List<Path> files = filesStream
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toList());
            return filesLinesStreamProvider
                    .lines(files)
                    .map(fromLineMapper)
                    .filter(Objects::nonNull)
                    .filter(filter)
                    .sorted(comparator)
                    .map(toLineMapper)
                    .parallel();
        }
    }

}
