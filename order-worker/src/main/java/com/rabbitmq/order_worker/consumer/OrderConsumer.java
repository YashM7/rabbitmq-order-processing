package com.rabbitmq.order_worker.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.order_worker.entity.Order;
import com.rabbitmq.order_worker.exception.InvalidOrderException;
import com.rabbitmq.order_worker.service.OrderProcessor;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.core.Message;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;

@Component
public class OrderConsumer {

    private final OrderProcessor orderProcessor;
    private final JsonMapper jsonMapper;

    public OrderConsumer(OrderProcessor orderProcessor, JsonMapper jsonMapper) {
        this.orderProcessor = orderProcessor;
        this.jsonMapper = jsonMapper;
    }

    @RabbitListener(queues = "order.queue")
    public void consumeOrder(Message message, Channel channel) throws Exception {

        String orderJson = new String(
                message.getBody(),
                StandardCharsets.UTF_8
        );

        Order order = jsonMapper.readValue(orderJson, Order.class);

        System.out.println("Received order: " + orderJson);

        long deliveryTag = message.getMessageProperties().getDeliveryTag();

        try {
            orderProcessor.process(order);
            channel.basicAck(deliveryTag, false);
            System.out.println("ACK sent for order: " + order.getOrderId());

        } catch (InvalidOrderException exception) {
            System.out.println("Invalid order, sending to DLQ: " + exception.getMessage());
            channel.basicNack(deliveryTag, false, false);
        }
    }
}