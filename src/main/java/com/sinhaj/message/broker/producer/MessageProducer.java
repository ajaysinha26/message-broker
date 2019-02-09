package com.sinhaj.message.broker.producer;

import com.sinhaj.message.broker.core.MessageCoordinator;

/**
 * Created by ajaysinha on 09/02/19.
 */
public class MessageProducer {

    private MessageCoordinator coordinator;

    public MessageProducer(MessageCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    public void produce(String topic, String message) {
        coordinator.sendMessage(topic, message);
    }
}
