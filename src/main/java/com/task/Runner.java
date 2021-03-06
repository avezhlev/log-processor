package com.task;

import com.task.domain.LogEntry;
import com.task.processor.LineBasedFilesProcessor;
import com.task.util.FilesLinesStreamProviderImpl;

import java.util.Comparator;

public class Runner {

    public static void main(String[] args) {
        if (args.length < 2) {
            printUsage();
            return;
        }
        LineBasedFilesProcessor<LogEntry> processor =
                new LineBasedFilesProcessor<>(
                        LogEntry::parse, LogEntry::toString,
                        new FilesLinesStreamProviderImpl((path, e) ->
                                System.out.println("Skipping failed to open file '" + path + "': " + e)));
        System.out.println("Processing...");
        try {
            long count = processor.process(
                    args[0], args[1],
                    logEntry -> logEntry.getSeverity().equals("ERROR"),
                    Comparator.comparing(LogEntry::getException));
            System.out.println("Processed successfully, found entries: " + count);
        } catch (Exception e) {
            System.out.println("Processing error: " + e);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar <jar_name> <log_files_dir_path> <output_file_path>");
    }

}
