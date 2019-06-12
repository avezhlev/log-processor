package com.task.processor;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class LogEntry {

    private static final String regex = "^" +
            "(?<datetime>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})" +
            "[\\s]+" +
            "(?<severity>[A-Z]+)" +
            "[\\s]+" +
            "(?<misc>.*?):\\s(?<exception>.*?)" +
            "$";
    private static final Pattern pattern = Pattern.compile(regex);

    private final String datetime;
    private final String severity;
    private final String misc;
    private final String exception;

    public static LogEntry parse(String line) {
        Matcher matcher = pattern.matcher(line);
        if (matcher.matches()) {
            return new LogEntry(
                    matcher.group("datetime"),
                    matcher.group("severity"),
                    matcher.group("misc"),
                    matcher.group("exception")
            );
        }
        return null;
    }

    @Override
    public String toString() {
        return datetime +
                "\t" +
                severity +
                "\t" +
                misc + ": " + exception;
    }

}
