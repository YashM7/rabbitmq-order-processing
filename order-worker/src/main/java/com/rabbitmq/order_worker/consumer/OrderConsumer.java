package com.rabbitmq.order_worker.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.order_worker.entity.Order;
import com.rabbitmq.order_worker.exception.InvalidOrderException;
import com.rabbitmq.order_worker.service.OrderProcessor;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static com.rabbitmq.order_worker.config.RabbitMQConfig.*;

@Component
public class OrderConsumer {

    private static final long MAX_RETRIES = 3;
    private final OrderProcessor orderProcessor;
    private final JsonMapper jsonMapper;
    private final RabbitTemplate rabbitTemplate;

    public OrderConsumer(
            OrderProcessor orderProcessor,
            JsonMapper jsonMapper,
            RabbitTemplate rabbitTemplate)
    {
        this.orderProcessor = orderProcessor;
        this.jsonMapper = jsonMapper;
        this.rabbitTemplate = rabbitTemplate;
    }

    private long getRetryCount(Message message) {

        Object xDeathHeader =
                message.getMessageProperties()
                        .getHeaders()
                        .get("x-death");

        if(!(xDeathHeader instanceof List<?> xDeathList)) {
            return 0;
        }

        for(Object deathEntry : xDeathList) {

           if(!(deathEntry instanceof Map<?,?> deathMap)) {
               continue;
           }

           Object queue = deathMap.get("queue");
           Object reason = deathMap.get("reason");
           Object count = deathMap.get("count");

            if ("order.retry".equals(queue)
                    && "expired".equals(reason)
                    && count instanceof Number number) {

                return number.longValue();
            }
        }

        return 0;
    }

    @RabbitListener(queues = "order.queue")
    public void consumeOrder(Message message, Channel channel) throws Exception {

        String orderJson = new String(
                message.getBody(),
                StandardCharsets.UTF_8
        );

        Order order = jsonMapper.readValue(orderJson, Order.class);

        System.out.println("Received order: " + orderJson);

        System.out.println("Message headers: " + message.getMessageProperties().getHeaders());

        System.out.println("Retry count: " + getRetryCount(message));

        long deliveryTag = message.getMessageProperties().getDeliveryTag();

        try {
            orderProcessor.process(order);
            channel.basicAck(deliveryTag, false);
            System.out.println("ACK sent for order: " + order.getOrderId());

        } catch (InvalidOrderException exception) {
            System.out.println("Permanent failure. Sent to DLQ: " + exception.getMessage());
            channel.basicNack(deliveryTag, false, false);

        } catch (Exception exception) {

            long retryCount = getRetryCount(message);

            if (retryCount < MAX_RETRIES) {

                System.out.println(
                        "Transient failure. Retry "
                                + (retryCount + 1)
                                + " of "
                                + MAX_RETRIES
                                + ": "
                                + order.getOrderId()
                );

                rabbitTemplate.send(
                        ORDER_EXCHANGE,
                        ORDER_RETRY_ROUTING_KEY,
                        message
                );

                channel.basicAck(deliveryTag, false);
                System.out.println("Message sent to retry queue");

            } else {

                System.out.println("Maximum retries reached. Sending to DLQ: " + order.getOrderId());

                rabbitTemplate.send(
                        ORDER_DLX,
                        ORDER_DLQ,
                        message
                );

                channel.basicAck(deliveryTag, false);
                System.out.println("Message sent to DLQ");
            }
        }
    }
}