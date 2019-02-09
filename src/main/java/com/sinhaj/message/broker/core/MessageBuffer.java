package com.sinhaj.message.broker.core;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by ajaysinha on 09/02/19.
 */
public class MessageBuffer {
    private String topic;
    private int capacity;
    private BlockingQueue<String> queue;

    public MessageBuffer(String topic, int capacity) {
        this.topic = topic;
        this.capacity = capacity;
        queue = new ArrayBlockingQueue<String>(capacity);
    }

    public String get() {
        return queue.poll();
    }

    public void put(String message) {
        try {
            queue.put(message);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public int size() {
        return queue.size();
    }
}
