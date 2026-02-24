package com.javaqueue.core;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.javaqueue.exception.QueueNotFoundException;

public class QueueManager {

    // ConcurrentHashMap handles its own thread safety internally.
    // We don't need synchronized here because the map itself guarantees
    // atomic put/get/remove operations across threads.
    private final ConcurrentHashMap<String, MessageQueue> queues = new ConcurrentHashMap<>();

    public MessageQueue createQueue(String name) {
        // computeIfAbsent is atomic — if two threads call createQueue("orders")
        // simultaneously, only one MessageQueue is created. Not two.
        return queues.computeIfAbsent(name, MessageQueue::new);
    }

    public MessageQueue getQueue(String name) {
        MessageQueue queue = queues.get(name);
        if (queue == null) {
            throw new QueueNotFoundException(name);
        }
        return queue;
    }

    public void deleteQueue(String name) {
        queues.remove(name);
    }

    public Set<String> listQueues() {
        return queues.keySet();
    }
}

/*
Why not this?
if (!queues.containsKey(name)) {
    queues.put(name, new MessageQueue(name));
}
```

Because that's two separate operations — `containsKey` and `put`. Another thread could slip in between them:
```
Thread 1: containsKey("orders") → false
Thread 2: containsKey("orders") → false      ← both see it missing
Thread 1: put("orders", new MessageQueue())
Thread 2: put("orders", new MessageQueue())  ← overwrites Thread 1's queue!

*/ 