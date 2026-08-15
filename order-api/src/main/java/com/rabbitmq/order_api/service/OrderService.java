package com.rabbitmq.order_api.service;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import static com.rabbitmq.order_api.config.RabbitMQConfig.ORDER_EXCHANGE;
import static com.rabbitmq.order_api.config.RabbitMQConfig.ORDER_ROUTING_KEY;

@Service
public class OrderService {

    private final RabbitTemplate rabbitTemplate;

    public OrderService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void createOrder(String order) {
        rabbitTemplate.convertAndSend(
                ORDER_EXCHANGE,
                ORDER_ROUTING_KEY,
                order
        );
    }
}