package com.javaqueue.exception;

/**
 * An offset that the log cannot serve — older than what retention has kept,
 * or beyond what has been appended.
 */
public class OffsetOutOfRangeException extends RuntimeException {

    public OffsetOutOfRangeException(String logName, long offset, long begin, long end) {
        super("Offset " + offset + " is out of range for log '" + logName
                + "' — retained range is [" + begin + ", " + end + "]");
    }
}
