package com.task.util;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class StreamUtils {

    /**
     * Provides a balanced tree of concatenated streams.
     *
     * @param streams A list of streams to concat.
     * @param <T>     The type of the stream elements.
     * @return A concatenated stream.
     */
    public static <T> Stream<T> balancingConcat(List<Stream<T>> streams) {
        return doBalancingConcat(streams, 0, streams.size());
    }

    private static <T> Stream<T> doBalancingConcat(List<Stream<T>> streams, int low, int high) {
        switch (high - low) {
            case 0:
                return Stream.empty();
            case 1:
                return streams.get(low);
            default:
                int mid = (low + high) >>> 1;
                Stream<T> left = doBalancingConcat(streams, low, mid);
                Stream<T> right = doBalancingConcat(streams, mid, high);
                return Stream.concat(left, right);
        }
    }

    /**
     * Provides a mapping result for usage in streams. Allows to properly handle exceptions.
     *
     * @param <T> Value type before mapping.
     * @param <R> Value type after mapping.
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class MappingResult<T, R> {

        private final T initialValue;
        private final Exception mappingException;
        private final R mappedValue;

        private static <T, R> MappingResult<T, R> success(R value) {
            return new MappingResult<>(null, null, value);
        }

        private static <T, R> MappingResult<T, R> failure(T initialValue, Exception exception) {
            return new MappingResult<>(initialValue, exception, null);
        }

        public static <T, R> Function<T, MappingResult<T, R>> wrap(CheckedFunction<T, R> function) {
            return t -> {
                try {
                    return success(function.apply(t));
                } catch (Exception e) {
                    return failure(t, e);
                }
            };
        }

        public boolean isSuccessful() {
            return mappedValue != null;
        }

        @FunctionalInterface
        public interface CheckedFunction<T, R> {
            R apply(T t) throws Exception;
        }

    }

}
