package com.rabbitmq.order_api.service;

import com.rabbitmq.order_api.DTO.OrderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

import static com.rabbitmq.order_api.config.RabbitMQConfig.ORDER_EXCHANGE;
import static com.rabbitmq.order_api.config.RabbitMQConfig.ORDER_ROUTING_KEY;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    private final RabbitTemplate rabbitTemplate;

    public OrderService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void createOrder(OrderRequest order) {
        String traceId = UUID.randomUUID().toString();

        log.info("Publishing order with traceId={} orderId={}", traceId, order.orderId());

        rabbitTemplate.convertAndSend(ORDER_EXCHANGE, ORDER_ROUTING_KEY, order, message -> {
            message.getMessageProperties().setHeader("traceId", traceId);
            return message;
        });
    }
}