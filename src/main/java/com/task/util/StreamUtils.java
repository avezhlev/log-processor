package com.task.util;

import java.util.List;
import java.util.stream.Stream;

public class StreamUtils {

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
}
