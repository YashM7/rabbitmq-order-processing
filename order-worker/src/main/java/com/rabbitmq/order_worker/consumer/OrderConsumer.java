package com.rabbitmq.order_worker.consumer;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.core.Message;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    @RabbitListener(queues = "order.queue")
    public void consumeOrder(Message message, Channel channel) throws Exception {

        String order = new String(message.getBody());

        System.out.println("Received order: " + order);

        channel.basicNack(
                message.getMessageProperties().getDeliveryTag(),
                false,
                false
        );
    }
}