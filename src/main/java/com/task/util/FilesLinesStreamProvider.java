package com.task.util;

import java.nio.file.Path;
import java.util.Collection;
import java.util.stream.Stream;

public interface FilesLinesStreamProvider {

    Stream<String> lines(Collection<? extends Path> files);
}
