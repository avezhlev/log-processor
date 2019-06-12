package com.task;

import com.task.processor.LogEntry;
import com.task.processor.LogFilesDirProcessor;

import java.util.Comparator;

public class Runner {

    public static void main(String[] args) {
        if (args.length < 2) {
            printUsage();
            return;
        }
        LogFilesDirProcessor<LogEntry> processor =
                new LogFilesDirProcessor<>(LogEntry::parse, LogEntry::toString);
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
