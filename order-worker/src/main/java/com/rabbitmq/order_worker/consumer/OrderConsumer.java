package com.rabbitmq.order_worker.consumer;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    @RabbitListener(queues = "order.queue")
    public void consumeOrder(String order) {
        
        System.out.println("Received order: " + order);
    }
}