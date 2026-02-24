package com.javaqueue.exception;

public class InvalidReceiptException extends RuntimeException {
    
    public InvalidReceiptException(String receiptHandle) {
        super("Invalid or already acknowledged receipt handle: " + receiptHandle);
    }
}
