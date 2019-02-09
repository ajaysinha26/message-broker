package com.sinhaj.message.broker;

import com.sinhaj.message.broker.core.MessageCoordinator;
import com.sinhaj.message.broker.producer.MessageProducer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by ajaysinha on 09/02/19.
 */
public class Run {

    public static void main(String[] args) {
        MessageCoordinator coordinator = new MessageCoordinator();
        coordinator.createTopic("Hello", 10000);
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for(int i = 0; i < 5; i++) {
            executorService.submit((Runnable) () -> {
                MessageProducer producer = new MessageProducer(coordinator);
                while (true) {
                    int counter = 1;
                    for(int i1 = 0; i1 < 100; i1++) {
                        producer.produce("Hello", Thread.currentThread().getName() + "-Message-" + counter++);
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        coordinator.subscribe("Hello", message -> System.out.println("***** " + message));
        coordinator.subscribe("Hello", message -> System.out.println("##### " + message));
        coordinator.subscribe("Hello", message -> System.out.println("@@@@@ " + message));
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        coordinator.shutDown();
    }
}
