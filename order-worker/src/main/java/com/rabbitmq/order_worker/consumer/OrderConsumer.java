package com.rabbitmq.order_worker.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.order_worker.entity.Order;
import com.rabbitmq.order_worker.exception.DuplicateOrderException;
import com.rabbitmq.order_worker.exception.PermanentOrderException;
import com.rabbitmq.order_worker.exception.TransientOrderException;
import com.rabbitmq.order_worker.service.OrderProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
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

    private static final long MAX_RETRIES = 5;
    private final OrderProcessor orderProcessor;
    private final JsonMapper jsonMapper;
    private final RabbitTemplate rabbitTemplate;
    private static final long BASE_DELAY_MS = 5000;
    private static final long MAX_DELAY_MS = 30000;
    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

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

    private long calculateBackoffDelay(long retryCount) {
        long delay = (long) (BASE_DELAY_MS * Math.pow(2, retryCount));
        return Math.min(delay, MAX_DELAY_MS);
    }

    private void handleRetryableFailure(
            String reason,
            boolean isUnanticipated,
            Message message,
            Channel channel,
            long deliveryTag,
            Order order) throws Exception {

        long retryCount = getRetryCount(message);

        if (retryCount < MAX_RETRIES) {

            long delay = calculateBackoffDelay(retryCount);
            System.out.println(reason + ". Retry " + (retryCount + 1) + " of " + MAX_RETRIES
                    + " for " + order.getOrderId() + " - delay " + delay + "ms");

            String logMessage = reason + ". Retry " + (retryCount + 1) + " of " + MAX_RETRIES
                    + " for " + order.getOrderId() + " - delay " + delay + "ms";

            if (isUnanticipated) {
                log.error(logMessage);
            } else {
                log.warn(logMessage);
            }

            message.getMessageProperties().setExpiration(String.valueOf(delay));

            rabbitTemplate.send(ORDER_EXCHANGE, ORDER_RETRY_ROUTING_KEY, message);
            channel.basicAck(deliveryTag, false);
        } else {
            System.out.println("Max retries reached. Sending to DLQ: " + order.getOrderId());
            String dlqMessage = "Max retries reached. Sending to DLQ: " + order.getOrderId();
            if (isUnanticipated) {
                log.error(dlqMessage);
            } else {
                log.warn(dlqMessage);
            }
            channel.basicNack(deliveryTag, false, false);
        }
    }

    @RabbitListener(queues = ORDER_QUEUE)
    public void consumeOrder(Message message, Channel channel) throws Exception {

        Object traceIdHeader = message.getMessageProperties().getHeaders().get("traceId");
        String traceId = traceIdHeader != null ? traceIdHeader.toString() : "no-trace-id";
        MDC.put("traceId", traceId);

        String orderJson = new String(
                message.getBody(),
                StandardCharsets.UTF_8
        );

        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        Order order;

        try {
            order = jsonMapper.readValue(orderJson, Order.class);
        } catch (Exception e) {
            log.warn("Malformed order message: " + e.getMessage());
            channel.basicNack(deliveryTag, false, false);
            return;
        }

        MDC.put("orderId", order.getOrderId());
        MDC.put("retryCount", String.valueOf(getRetryCount(message)));
        MDC.put("redelivered", String.valueOf(message.getMessageProperties().isRedelivered()));

        log.info("Received order: " + orderJson);

        log.info("Message headers: " + message.getMessageProperties().getHeaders());

        log.info("Retry count: " + getRetryCount(message));

        try {
            log.info("Received order: " + orderJson);

            try {
                orderProcessor.processWithUpsert(order);
                channel.basicAck(deliveryTag, false);
                log.info("ACK sent for order: " + order.getOrderId());

            } catch (DuplicateOrderException exception) {
                channel.basicAck(deliveryTag, false);
                log.warn("Duplicate detected, already processed elsewhere — acking without action. " + exception.getMessage());

            } catch (PermanentOrderException exception) {
                log.warn("Permanent failure. Sent to DLQ: " + exception.getMessage());
                channel.basicNack(deliveryTag, false, false);

            } catch (TransientOrderException exception) {
                handleRetryableFailure("Transient failure", false, message, channel, deliveryTag, order);

            } catch (Exception exception) {
                handleRetryableFailure("Unclassified failure (" + exception.getMessage() + ")", true, message, channel, deliveryTag, order);
            }
        } finally {
            MDC.clear();
        }

    }
}