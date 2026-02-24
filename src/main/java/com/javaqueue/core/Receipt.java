package com.javaqueue.core;

import java.util.UUID;

public class Receipt {

    // UUID gives us a globally unique handle.
    // This is what the consumer hands back when calling acknowledge().
    private final String receiptHandle;
    private final Message message;

    public Receipt(Message message) {
        this.receiptHandle = UUID.randomUUID().toString();
        this.message = message;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }

    public Message getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "Receipt{handle='" + receiptHandle + "', message=" + message + "}";
    }
}
