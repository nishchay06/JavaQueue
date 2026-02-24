package com.javaqueue;

import com.javaqueue.core.Message;
import com.javaqueue.core.MessageQueue;
import com.javaqueue.core.QueueManager;
import com.javaqueue.core.Receipt;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        QueueManager manager = new QueueManager();
        MessageQueue queue = manager.createQueue("orders");

        // Producer
        queue.publish(new Message("Order #1"));
        queue.publish(new Message("Order #2"));

        // Consumer
        Receipt r1 = queue.consume();
        System.out.println("Consumed: " + r1.getMessage());
        queue.acknowledge(r1.getReceiptHandle());

        Receipt r2 = queue.consume();
        System.out.println("Consumed: " + r2.getMessage());
        queue.acknowledge(r2.getReceiptHandle());
    }
}
