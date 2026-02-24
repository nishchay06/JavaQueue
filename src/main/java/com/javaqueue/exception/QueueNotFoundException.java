package com.javaqueue.exception;

public class QueueNotFoundException extends RuntimeException {
    
    public QueueNotFoundException(String queueName) {
        super("Queue not found: " + queueName);
    }
}
