package com.task.util;

import com.task.util.StreamUtils.MappingResult;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.RandomAccess;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@NoArgsConstructor(force = true)
@RequiredArgsConstructor
public class FilesLinesStreamProviderImpl implements FilesLinesStreamProvider {

    // Make sure that this value is less than OS limit on open file descriptors per process
    private final static int MAX_ALLOWED_OPEN_FILE_DESCRIPTORS = 128;

    private final BiConsumer<? super Path, ? super Exception> failedFileOpeningHandler;

    @Override
    public Stream<String> lines(Collection<? extends Path> files) {
        return files.size() > MAX_ALLOWED_OPEN_FILE_DESCRIPTORS ?
                flatMappedPipeline(files) :
                concatenatedPipeline(files);
    }

    /**
     * Provides better parallelism, especially if used with JDK 9+
     * (allowing to process groups of lines from the same file by different threads),
     * but implies trade-off in a form of a large number of simultaneously open file descriptors.
     *
     * @param files A collection of file paths.
     * @return A stream of lines from the specified files.
     */
    private Stream<String> concatenatedPipeline(Collection<? extends Path> files) {
        return initialPipeline(files)
                .map(MappingResult::getMappedValue)
                .collect(Collectors.collectingAndThen(
                        Collectors.toList(),
                        StreamUtils::balancingConcat));
    }

    /**
     * Provides worse parallelism (a file is fully processed by one thread due to flat mapping),
     * but also constrains the number of simultaneously open file descriptors to the number of processing threads.
     *
     * @param files A collection of file paths.
     * @return A stream of lines from the specified files.
     */
    private Stream<String> flatMappedPipeline(Collection<? extends Path> files) {
        return initialPipeline(files)
                .flatMap(MappingResult::getMappedValue);
    }

    /**
     * Common initial pipeline that wraps mapping results to properly store and handle file openings exceptions.
     *
     * @param files A collection of file paths. The provided collection is converted to {@link ArrayList}
     *              if it is not already an instance of {@link RandomAccess},
     *              since spliterators provided by RandomAccess implementations (at least JDK's ones)
     *              provide excellent task splitting capabilities.
     * @return A stream of successful attempts to map a file to its lines stream.
     */
    private Stream<? extends MappingResult<? extends Path, Stream<String>>> initialPipeline(Collection<? extends Path> files) {
        Stream<? extends MappingResult<? extends Path, Stream<String>>> result =
                (files instanceof RandomAccess ? files : new ArrayList<>(files))
                        .stream()
                        .map(MappingResult.wrap(Files::lines));
        if (failedFileOpeningHandler != null) {
            result = result.peek(this::handleFileOpeningResultIfFailed);
        }
        return result.filter(MappingResult::isSuccessful);
    }

    @SuppressWarnings("ConstantConditions")
    private void handleFileOpeningResultIfFailed(MappingResult<? extends Path, Stream<String>> result) {
        if (!result.isSuccessful()) {
            failedFileOpeningHandler.accept(result.getInitialValue().toAbsolutePath(), result.getMappingException());
        }
    }

}
