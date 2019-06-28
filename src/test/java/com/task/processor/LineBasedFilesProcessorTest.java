package com.task.processor;

import com.task.domain.LogEntry;
import com.task.util.FilesLinesStreamProviderImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class LineBasedFilesProcessorTest {

    private static final String TEST_DATA_DIR_PATH = "./data";
    private static final String TEST_RESULT_FILE_PATH = "./data/output/out.log";
    private static final long[] TEST_DATA_ENTRIES_PER_LOG_FILE = new long[]{292, 141, 698};
    private static final long[] TEST_DATA_INFO_ENTRIES_PER_LOG_FILE = new long[]{88, 31, 118};

    private static final long TEST_DATA_TOTAL_ENTRIES = Arrays.stream(TEST_DATA_ENTRIES_PER_LOG_FILE).sum();
    private static final long TEST_DATA_TOTAL_INFO_ENTRIES = Arrays.stream(TEST_DATA_INFO_ENTRIES_PER_LOG_FILE).sum();

    private LineBasedFilesProcessor<LogEntry> objectUnderTest;

    @Before
    public void setup() {
        objectUnderTest = new LineBasedFilesProcessor<>(LogEntry::parse, LogEntry::toString, new FilesLinesStreamProviderImpl());
    }

    @Test
    public void testProcessingWithoutFiltering() throws IOException {
        // given: setup
        // when
        long count = objectUnderTest.process(
                TEST_DATA_DIR_PATH, TEST_RESULT_FILE_PATH,
                logEntry -> true,
                (o1, o2) -> 0);
        //then
        Assert.assertEquals(TEST_DATA_TOTAL_ENTRIES, count);
        Assert.assertEquals(TEST_DATA_TOTAL_ENTRIES, Files.lines(Paths.get(TEST_RESULT_FILE_PATH)).count());
    }

    @Test
    public void testProcessingWithFiltering() throws IOException {
        // given: setup
        // when
        long count = objectUnderTest.process(
                TEST_DATA_DIR_PATH, TEST_RESULT_FILE_PATH,
                logEntry -> logEntry.getSeverity().equals("INFO"),
                (o1, o2) -> 0);
        // then
        Assert.assertEquals(TEST_DATA_TOTAL_INFO_ENTRIES, count);
        Assert.assertEquals(TEST_DATA_TOTAL_INFO_ENTRIES, Files.lines(Paths.get(TEST_RESULT_FILE_PATH)).count());
    }

}
