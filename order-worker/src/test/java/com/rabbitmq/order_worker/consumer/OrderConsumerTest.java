package com.rabbitmq.order_worker.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.order_worker.config.RabbitMQConfig;
import com.rabbitmq.order_worker.entity.Order;
import com.rabbitmq.order_worker.exception.DuplicateOrderException;
import com.rabbitmq.order_worker.exception.PermanentOrderException;
import com.rabbitmq.order_worker.exception.TransientOrderException;
import com.rabbitmq.order_worker.service.OrderProcessor;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderConsumerTest {

    @Mock
    private OrderProcessor orderProcessor;

    @Mock
    private JsonMapper jsonMapper;

    @Mock
    private RabbitTemplate rabbitTemplate;

    @Mock
    private Channel channel;

    @InjectMocks
    private OrderConsumer orderConsumer;

    private Message message;
    private MessageProperties messageProperties;
    private Order order;

    private final String orderJson =
            "{\"orderId\":\"ORDER-123\",\"status\":\"NEW\"}";

    private final long deliveryTag = 12345L;

    @BeforeEach
    void setUp() {
        MDC.clear();

        messageProperties = new MessageProperties();
        messageProperties.setDeliveryTag(deliveryTag);

        message = new Message(
                orderJson.getBytes(StandardCharsets.UTF_8),
                messageProperties
        );

        order = new Order();
        order.setOrderId("ORDER-123");
    }

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    private void mockValidOrder() throws Exception {
        when(jsonMapper.readValue(orderJson, Order.class))
                .thenReturn(order);
    }

    private void setRetryCount(long count) {
        Map<String, Object> deathEntry = Map.of(
                "queue", "order.retry",
                "reason", "expired",
                "count", count
        );

        messageProperties.setHeader("x-death", List.of(deathEntry));
    }

    private void verifyRetryPublished(String expectedExpiration) throws IOException {
        assertThat(messageProperties.getExpiration())
                .isEqualTo(expectedExpiration);

        verify(rabbitTemplate).send(
                eq(RabbitMQConfig.ORDER_EXCHANGE),
                eq(RabbitMQConfig.ORDER_RETRY_ROUTING_KEY),
                eq(message)
        );

        verify(channel).basicAck(deliveryTag, false);

        verify(channel, never())
                .basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    private void verifyMdcCleared() {
        assertThat(MDC.get("traceId")).isNull();
        assertThat(MDC.get("orderId")).isNull();
        assertThat(MDC.get("retryCount")).isNull();
        assertThat(MDC.get("redelivered")).isNull();
    }

    @Test
    @DisplayName("Valid order is processed and acknowledged")
    void consumeOrder_success_sendsAck() throws Exception {
        mockValidOrder();
        messageProperties.setHeader("traceId", "trace-123");

        orderConsumer.consumeOrder(message, channel);

        verify(orderProcessor).processWithUpsert(order);
        verify(channel).basicAck(deliveryTag, false);
        verify(channel, never())
                .basicNack(anyLong(), anyBoolean(), anyBoolean());
        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    @DisplayName("Missing trace ID header does not prevent processing")
    void consumeOrder_missingTraceId_stillProcessesOrder()
            throws Exception {
        mockValidOrder();

        orderConsumer.consumeOrder(message, channel);

        verify(orderProcessor).processWithUpsert(order);
        verify(channel).basicAck(deliveryTag, false);
    }

    @Test
    @DisplayName("Successful processing clears MDC")
    void consumeOrder_success_clearsMdc() throws Exception {
        mockValidOrder();
        messageProperties.setHeader("traceId", "trace-123");

        orderConsumer.consumeOrder(message, channel);

        verifyMdcCleared();
    }

    @Test
    @DisplayName("Redelivered message can be processed and acknowledged")
    void consumeOrder_redeliveredMessage_processesOrder()
            throws Exception {
        mockValidOrder();
        messageProperties.setRedelivered(true);

        orderConsumer.consumeOrder(message, channel);

        verify(orderProcessor).processWithUpsert(order);
        verify(channel).basicAck(deliveryTag, false);
    }

    @Test
    @DisplayName("Malformed JSON is rejected without requeue")
    void consumeOrder_malformedJson_sendsNack() throws Exception {
        when(jsonMapper.readValue(orderJson, Order.class))
                .thenThrow(new RuntimeException("Invalid JSON"));

        orderConsumer.consumeOrder(message, channel);

        verify(channel).basicNack(deliveryTag, false, false);
        verifyNoInteractions(orderProcessor);
        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    @DisplayName("Duplicate exception is acknowledged without retry")
    void consumeOrder_duplicateException_sendsAck() throws Exception {
        mockValidOrder();

        doThrow(new DuplicateOrderException("Duplicate order"))
                .when(orderProcessor).processWithUpsert(order);

        orderConsumer.consumeOrder(message, channel);

        verify(channel).basicAck(deliveryTag, false);
        verify(channel, never())
                .basicNack(anyLong(), anyBoolean(), anyBoolean());
        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    @DisplayName("Permanent failure is rejected without requeue")
    void consumeOrder_permanentFailure_sendsNack() throws Exception {
        mockValidOrder();

        doThrow(new PermanentOrderException("Invalid order"))
                .when(orderProcessor).processWithUpsert(order);

        orderConsumer.consumeOrder(message, channel);

        verify(channel).basicNack(deliveryTag, false, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    @DisplayName("Transient failure is sent to retry mechanism")
    void consumeOrder_transientFailure_publishesRetry()
            throws Exception {
        mockValidOrder();

        doThrow(new TransientOrderException("Database unavailable"))
                .when(orderProcessor).processWithUpsert(order);

        orderConsumer.consumeOrder(message, channel);

        verifyRetryPublished("5000");
    }

    @Test
    @DisplayName("Unexpected exception is sent to retry mechanism")
    void consumeOrder_unclassifiedFailure_publishesRetry()
            throws Exception {
        mockValidOrder();

        doThrow(new IllegalStateException("Unexpected failure"))
                .when(orderProcessor).processWithUpsert(order);

        orderConsumer.consumeOrder(message, channel);

        verifyRetryPublished("5000");
    }

    @Test
    @DisplayName("Processing failure clears MDC")
    void consumeOrder_processingFailure_clearsMdc()
            throws Exception {
        mockValidOrder();
        messageProperties.setHeader("traceId", "trace-failure");

        doThrow(new TransientOrderException("Database unavailable"))
                .when(orderProcessor).processWithUpsert(order);

        orderConsumer.consumeOrder(message, channel);

        verifyMdcCleared();
    }

    @Test
    @DisplayName("Retry count zero produces a five-second delay")
    void consumeOrder_firstRetry_usesFiveSecondDelay()
            throws Exception {
        mockValidOrder();

        doThrow(new TransientOrderException("DB timeout"))
                .when(orderProcessor).processWithUpsert(order);

        orderConsumer.consumeOrder(message, channel);

        verifyRetryPublished("5000");
    }

    @Test
    @DisplayName("Retry count two produces a twenty-second delay")
    void consumeOrder_retryCountTwo_usesTwentySecondDelay()
            throws Exception {
        mockValidOrder();
        setRetryCount(2);

        doThrow(new TransientOrderException("DB timeout"))
                .when(orderProcessor).processWithUpsert(order);

        orderConsumer.consumeOrder(message, channel);

        verifyRetryPublished("20000");
    }

    @Test
    @DisplayName("Retry delay is capped at thirty seconds")
    void consumeOrder_highRetryCount_capsDelay()
            throws Exception {
        mockValidOrder();
        setRetryCount(4);

        doThrow(new TransientOrderException("DB timeout"))
                .when(orderProcessor).processWithUpsert(order);

        orderConsumer.consumeOrder(message, channel);

        verifyRetryPublished("30000");
    }

    @Test
    @DisplayName("Retry count five sends message to DLQ")
    void consumeOrder_maxRetriesReached_sendsNack()
            throws Exception {
        mockValidOrder();
        setRetryCount(5);

        doThrow(new TransientOrderException("DB timeout"))
                .when(orderProcessor).processWithUpsert(order);

        orderConsumer.consumeOrder(message, channel);

        verify(channel).basicNack(deliveryTag, false, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    @DisplayName("Unexpected exception also goes to DLQ after max retries")
    void consumeOrder_unclassifiedFailureAtMaxRetries_sendsNack()
            throws Exception {
        mockValidOrder();
        setRetryCount(5);

        doThrow(new IllegalStateException("Unexpected failure"))
                .when(orderProcessor).processWithUpsert(order);

        orderConsumer.consumeOrder(message, channel);

        verify(channel).basicNack(deliveryTag, false, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    @DisplayName("Missing x-death header starts retry count at zero")
    void consumeOrder_missingXDeathHeader_startsAtZero()
            throws Exception {
        mockValidOrder();

        doThrow(new TransientOrderException("DB timeout"))
                .when(orderProcessor).processWithUpsert(order);

        orderConsumer.consumeOrder(message, channel);

        verifyRetryPublished("5000");
    }

    @Test
    @DisplayName("Malformed x-death header starts retry count at zero")
    void consumeOrder_malformedXDeathHeader_startsAtZero()
            throws Exception {
        mockValidOrder();
        messageProperties.setHeader("x-death", "unexpected-header-value");

        doThrow(new TransientOrderException("DB timeout"))
                .when(orderProcessor).processWithUpsert(order);

        orderConsumer.consumeOrder(message, channel);

        verifyRetryPublished("5000");
    }

    @Test
    @DisplayName("Unrelated x-death entry does not increase retry count")
    void consumeOrder_unrelatedXDeathEntry_startsAtZero()
            throws Exception {
        mockValidOrder();

        messageProperties.setHeader("x-death", List.of(
                Map.of(
                        "queue", "order.queue",
                        "reason", "rejected",
                        "count", 4L
                )
        ));

        doThrow(new TransientOrderException("DB timeout"))
                .when(orderProcessor).processWithUpsert(order);

        orderConsumer.consumeOrder(message, channel);

        verifyRetryPublished("5000");
    }

    @Test
    @DisplayName("Retry publishing failure propagates and does not ACK")
    void consumeOrder_retryPublishFails_doesNotAck()
            throws Exception {
        mockValidOrder();

        doThrow(new TransientOrderException("DB timeout"))
                .when(orderProcessor).processWithUpsert(order);

        doThrow(new IllegalStateException("Broker unavailable"))
                .when(rabbitTemplate)
                .send(
                        eq(RabbitMQConfig.ORDER_EXCHANGE),
                        eq(RabbitMQConfig.ORDER_RETRY_ROUTING_KEY),
                        eq(message)
                );

        assertThrows(
                IllegalStateException.class,
                () -> orderConsumer.consumeOrder(message, channel)
        );

        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        verify(channel, never())
                .basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    @DisplayName("ACK failure after successful processing propagates")
    void consumeOrder_ackFails_propagatesException()
            throws Exception {
        mockValidOrder();

        doThrow(new IOException("Channel closed"))
                .when(channel).basicAck(deliveryTag, false);

        assertThrows(
                IOException.class,
                () -> orderConsumer.consumeOrder(message, channel)
        );

        verify(orderProcessor).processWithUpsert(order);
    }

    @Test
    @DisplayName("NACK failure for permanent error propagates")
    void consumeOrder_permanentFailureNackFails_propagatesException()
            throws Exception {
        mockValidOrder();

        doThrow(new PermanentOrderException("Invalid order"))
                .when(orderProcessor).processWithUpsert(order);

        doThrow(new IOException("Channel closed"))
                .when(channel).basicNack(deliveryTag, false, false);

        assertThrows(
                IOException.class,
                () -> orderConsumer.consumeOrder(message, channel)
        );
    }

    @Test
    @DisplayName("MDC is cleared when maximum retries are reached")
    void consumeOrder_maxRetriesReached_clearsMdc()
            throws Exception {
        mockValidOrder();
        setRetryCount(5);
        messageProperties.setHeader("traceId", "trace-max-retries");

        doThrow(new TransientOrderException("DB timeout"))
                .when(orderProcessor).processWithUpsert(order);

        orderConsumer.consumeOrder(message, channel);

        verifyMdcCleared();
    }

    @Test
    @DisplayName("Should ignore unrelated x-death entries when calculating retry count")
    void consumeOrder_TransientFailure_IgnoresUnrelatedDeathEntries()
            throws Exception {

        when(jsonMapper.readValue(orderJson, Order.class))
                .thenReturn(order);

        doThrow(new TransientOrderException("DB timeout"))
                .when(orderProcessor)
                .processWithUpsert(order);

        Map<String, Object> unrelatedEntry = Map.of(
                "queue", "order.queue",
                "reason", "rejected",
                "count", 10L
        );

        Map<String, Object> retryEntry = Map.of(
                "queue", "order.retry",
                "reason", "expired",
                "count", 3L
        );

        messageProperties.setHeader(
                "x-death",
                List.of(unrelatedEntry, retryEntry)
        );

        orderConsumer.consumeOrder(message, channel);

        // Three completed retry expirations -> 30-second capped delay.
        assertThat(messageProperties.getExpiration()).isEqualTo("30000");

        verify(rabbitTemplate).send(
                eq(RabbitMQConfig.ORDER_EXCHANGE),
                eq(RabbitMQConfig.ORDER_RETRY_ROUTING_KEY),
                eq(message)
        );

        verify(channel).basicAck(deliveryTag, false);
    }

    @Test
    @DisplayName("Should not ACK original message when retry publishing fails")
    void consumeOrder_RetryPublishFails_DoesNotAckOriginalMessage()
            throws Exception {

        // Arrange: JSON parsing succeeds, but processing fails transiently.
        when(jsonMapper.readValue(orderJson, Order.class))
                .thenReturn(order);

        doThrow(new TransientOrderException("Database unavailable"))
                .when(orderProcessor)
                .processWithUpsert(order);

        // Simulate RabbitTemplate failing to publish the retry message.
        doThrow(new RuntimeException("RabbitMQ unavailable"))
                .when(rabbitTemplate)
                .send(
                        eq(RabbitMQConfig.ORDER_EXCHANGE),
                        eq(RabbitMQConfig.ORDER_RETRY_ROUTING_KEY),
                        eq(message)
                );

        // Act + Assert: the publishing exception should propagate.
        assertThrows(
                RuntimeException.class,
                () -> orderConsumer.consumeOrder(message, channel)
        );

        // The original message must not be acknowledged or rejected.
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        verify(channel, never()).basicNack(
                anyLong(), anyBoolean(), anyBoolean()
        );

        // Verify that retry publishing was attempted.
        verify(rabbitTemplate).send(
                eq(RabbitMQConfig.ORDER_EXCHANGE),
                eq(RabbitMQConfig.ORDER_RETRY_ROUTING_KEY),
                eq(message)
        );
    }

    @Test
    @DisplayName("Should use 10-second delay for retry count 1")
    void consumeOrder_TransientFailure_RetryCountOne_UsesTenSecondDelay()
            throws Exception {

        when(jsonMapper.readValue(orderJson, Order.class))
                .thenReturn(order);

        doThrow(new TransientOrderException("Database unavailable"))
                .when(orderProcessor)
                .processWithUpsert(order);

        Map<String, Object> deathEntry = Map.of(
                "queue", "order.retry",
                "reason", "expired",
                "count", 1L
        );

        messageProperties.setHeader("x-death", List.of(deathEntry));

        orderConsumer.consumeOrder(message, channel);

        assertThat(messageProperties.getExpiration()).isEqualTo("10000");

        verify(rabbitTemplate).send(
                eq(RabbitMQConfig.ORDER_EXCHANGE),
                eq(RabbitMQConfig.ORDER_RETRY_ROUTING_KEY),
                eq(message)
        );

        verify(channel).basicAck(deliveryTag, false);
    }

    @Test
    @DisplayName("Should default to zero retries when x-death header is malformed")
    void consumeOrder_MalformedDeathHeader_DefaultsToZeroRetries()
            throws Exception {

        when(jsonMapper.readValue(orderJson, Order.class))
                .thenReturn(order);

        doThrow(new TransientOrderException("Database unavailable"))
                .when(orderProcessor)
                .processWithUpsert(order);

        // Simulate an unexpected header format.
        messageProperties.setHeader("x-death", "unexpected-header-value");

        orderConsumer.consumeOrder(message, channel);

        // No valid retry entry means retryCount = 0.
        assertThat(messageProperties.getExpiration()).isEqualTo("5000");

        verify(rabbitTemplate).send(
                eq(RabbitMQConfig.ORDER_EXCHANGE),
                eq(RabbitMQConfig.ORDER_RETRY_ROUTING_KEY),
                eq(message)
        );

        verify(channel).basicAck(deliveryTag, false);
    }

    @Test
    @DisplayName("Should populate MDC with message context during processing")
    void consumeOrder_Success_PopulatesMdcDuringProcessing()
            throws Exception {

        when(jsonMapper.readValue(orderJson, Order.class))
                .thenReturn(order);

        messageProperties.setHeader("traceId", "trace-test-456");
        messageProperties.setHeader(
                "x-death",
                List.of(Map.of(
                        "queue", "order.retry",
                        "reason", "expired",
                        "count", 2L
                ))
        );
        messageProperties.setRedelivered(true);

        doAnswer(invocation -> {
            assertThat(MDC.get("traceId")).isEqualTo("trace-test-456");
            assertThat(MDC.get("orderId")).isEqualTo("ORDER-123");
            assertThat(MDC.get("retryCount")).isEqualTo("2");
            assertThat(MDC.get("redelivered")).isEqualTo("true");

            return null;
        }).when(orderProcessor).processWithUpsert(order);

        orderConsumer.consumeOrder(message, channel);

        verify(channel).basicAck(deliveryTag, false);

        // Context must be removed after processing finishes.
        assertThat(MDC.get("traceId")).isNull();
        assertThat(MDC.get("orderId")).isNull();
        assertThat(MDC.get("retryCount")).isNull();
        assertThat(MDC.get("redelivered")).isNull();
    }

    @Test
    @DisplayName("Should clear MDC when retry publishing fails")
    void consumeOrder_RetryPublishFails_ClearsMdc()
            throws Exception {

        when(jsonMapper.readValue(orderJson, Order.class))
                .thenReturn(order);

        messageProperties.setHeader("traceId", "trace-publish-failure");

        doThrow(new TransientOrderException("Database unavailable"))
                .when(orderProcessor)
                .processWithUpsert(order);

        doThrow(new RuntimeException("RabbitMQ unavailable"))
                .when(rabbitTemplate)
                .send(
                        eq(RabbitMQConfig.ORDER_EXCHANGE),
                        eq(RabbitMQConfig.ORDER_RETRY_ROUTING_KEY),
                        eq(message)
                );

        org.junit.jupiter.api.Assertions.assertThrows(
                RuntimeException.class,
                () -> orderConsumer.consumeOrder(message, channel)
        );

        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        verify(channel, never()).basicNack(
                anyLong(), anyBoolean(), anyBoolean()
        );

        assertThat(MDC.get("traceId")).isNull();
        assertThat(MDC.get("orderId")).isNull();
        assertThat(MDC.get("retryCount")).isNull();
        assertThat(MDC.get("redelivered")).isNull();
    }
}