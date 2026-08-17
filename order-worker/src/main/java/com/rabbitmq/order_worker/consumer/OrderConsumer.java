package com.rabbitmq.order_worker.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.order_worker.service.OrderProcessor;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.core.Message;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    private final OrderProcessor orderProcessor;

    public OrderConsumer(OrderProcessor orderProcessor) {
        this.orderProcessor = orderProcessor;
    }

    @RabbitListener(queues = "order.queue")
    public void consumeOrder(Message message, Channel channel) throws Exception {

        String order = new String(message.getBody());

        System.out.println("Received order: " + order);

        orderProcessor.process(order);

        channel.basicAck(
                message.getMessageProperties().getDeliveryTag(),
                false
        );

//        channel.basicNack(
//                message.getMessageProperties().getDeliveryTag(),
//                false,
//                false
//        );
    }
}