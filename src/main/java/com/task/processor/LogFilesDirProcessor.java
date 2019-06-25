package com.task.processor;

import com.task.util.StreamUtils;
import com.task.util.StreamUtils.MappingResult;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class LogFilesDirProcessor<T> {

    // This value should correlate with and be less than OS limit on open file descriptors per process.
    // Be warned that the algorithm uses common ForkJoinPool,
    // so make sure that at least (ForkJoinPool.getCommonPoolParallelism() + 1) file descriptors are allowed by OS.
    private final static int MAX_ALLOWED_OPEN_FILE_DESCRIPTORS = Math.max(512, ForkJoinPool.getCommonPoolParallelism() + 1);

    private final Function<String, ? extends T> fromLineMapper;
    private final Function<? super T, String> toLineMapper;

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
            return upstreamPipeline(files)
                    .map(fromLineMapper)
                    .filter(Objects::nonNull)
                    .filter(filter)
                    .sorted(comparator)
                    .map(toLineMapper)
                    .parallel();
        }
    }

    private Stream<String> upstreamPipeline(List<Path> files) {
        return files.size() > MAX_ALLOWED_OPEN_FILE_DESCRIPTORS ?
                flatMappedPipeline(files) :
                concatenatedPipeline(files);
    }

    /**
     * Provides better parallelism, especially if used with JDK 9+
     * (allowing to process groups of lines from the same file by different threads),
     * but implies trade-off in a form of larger number of simultaneously open file descriptors.
     *
     * @param files A list of file paths.
     * @return A stream of lines from the specified files.
     */
    private Stream<String> concatenatedPipeline(List<Path> files) {
        return initialPipeline(files)
                .map(MappingResult::getMappedValue)
                .collect(Collectors.collectingAndThen(
                        Collectors.toList(),
                        StreamUtils::balancingConcat));
    }

    /**
     * Provides worse parallelism (a file is fully processed by one thread due to flat-mapping),
     * but also constrains the number of simultaneously open file descriptors to the number of processing threads.
     *
     * @param files A list of file paths.
     * @return A stream of lines from the specified files.
     */
    private Stream<String> flatMappedPipeline(List<Path> files) {
        return initialPipeline(files)
                .flatMap(MappingResult::getMappedValue);
    }

    /**
     * Common initial pipeline that wraps mapping results to properly handle and log exceptions.
     *
     * @param files A list of file paths. The {@link List} interface is used on purpose,
     *              since spliterators provided by JDK list implementations provide good task splitting capabilities.
     * @return A stream of successful attempts to map a file to its lines stream.
     */
    private Stream<MappingResult<Path, Stream<String>>> initialPipeline(List<Path> files) {
        return files
                .stream()
                .map(MappingResult.wrap(Files::lines))
                .peek(this::logStreamOpeningResultIfFailed)
                .filter(MappingResult::isSuccessful);
    }

    private void logStreamOpeningResultIfFailed(MappingResult<Path, Stream<String>> result) {
        if (!result.isSuccessful()) {
            System.out.println("Skipping failed to open file " +
                    "'" + result.getInitialValue().toAbsolutePath() + "': " + result.getMappingException());
        }
    }

}
