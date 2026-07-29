package com.javaqueue.exception;

public class LogNotFoundException extends RuntimeException {

    public LogNotFoundException(String logName) {
        super("Log not found: " + logName);
    }
}
