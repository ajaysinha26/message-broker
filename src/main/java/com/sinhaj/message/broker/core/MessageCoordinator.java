package com.sinhaj.message.broker.core;

import com.sinhaj.message.broker.consumer.MessageConsumer;
import com.sinhaj.message.broker.core.MessageBuffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by ajaysinha on 09/02/19.
 */
public class MessageCoordinator {

    private Map<String, MessageBuffer> topicBufferMap = new HashMap<>();
    private Map<String, List<MessageConsumer>> topicSubscribers = new HashMap<>();
    private ExecutorService executorService = Executors.newFixedThreadPool(10);
    private Map<String, MessageListener> messageListenerMap = new HashMap<>();
    private volatile AtomicBoolean running = new AtomicBoolean(true);

    public synchronized void createTopic(String topic, int capacity) {
        System.out.println("Creating Topic: " + topic);
        if(!running.get()) {
            throw new RuntimeException("Stop signal has been received");
        }
        if(!topicBufferMap.containsKey(topic)) {
            topicBufferMap.put(topic, new MessageBuffer(topic, capacity));
            System.out.println("Created Message Buffer for Topic: " + topic);
            MessageListener messageListener = new MessageListener(topic);
            executorService.submit(messageListener);
            System.out.println("Created Message Listener for Topic: " + topic);
            messageListenerMap.put(topic, messageListener);
            System.out.println("Created Topic: " + topic);
        }
        System.out.println("Already exists Topic: " + topic);
    }

    public synchronized void shutDown() {
        System.out.println("Shutting down coordinator!");
        running.set(false);
        topicBufferMap.entrySet().stream().forEach(entry -> {
            System.out.println("Stopping listener for topic: " + entry.getKey());
            while (entry.getValue().size() != 0) {
                System.out.println("~~~~PENDING MESSAGES IN TOPIC: " + entry.getKey() + " is " + entry.getValue().size());
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            messageListenerMap.get(entry.getKey()).shutDown();
            System.out.println("Stopped listener for topic: " + entry.getKey());
        });
        executorService.shutdownNow();
        System.out.println("Coordinator shut down successfully");
    }

    public void sendMessage(String topic, String message) {
        if(!running.get()) {
            throw new RuntimeException("Stop signal has been received");
        }
        if(!topicBufferMap.containsKey(topic)) {
            throw new RuntimeException("Topic doesn't exist");
        }
        topicBufferMap.get(topic).put(message);
    }

    public synchronized void subscribe(String topic, MessageConsumer consumer) {
        if(!topicSubscribers.containsKey(topic)) {
            topicSubscribers.put(topic, new ArrayList<>());
        }
        List<MessageConsumer> subscribers = topicSubscribers.get(topic);
        subscribers.add(consumer);
    }

    class MessageListener implements Runnable {
        private String topic;
        private volatile AtomicBoolean running = new AtomicBoolean(true);;

        public MessageListener(String topic) {
            this.topic = topic;
        }

        @Override
        public void run() {
            MessageBuffer messageBuffer = topicBufferMap.get(topic);
            while (running.get()) {
                List<MessageConsumer> consumers = topicSubscribers.get(topic);
                if(consumers != null) {
                    consumers.stream().forEach(consumer -> consumer.consume(messageBuffer.get()));
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public void shutDown() {
            running.set(false);
        }
    }
}
