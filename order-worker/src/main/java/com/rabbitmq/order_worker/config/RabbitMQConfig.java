package com.rabbitmq.order_worker.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class RabbitMQConfig {

    public static final String ORDER_QUEUE = "order.queue";
    public static final String ORDER_EXCHANGE = "order.exchange";
    public static final String ORDER_ROUTING_KEY = "order.created";

    public static final String ORDER_DLX = "order.dlx";
    public static final String ORDER_DLQ = "order.dlq";

    @Bean
    public DirectExchange orderExchange() {
        return new DirectExchange(ORDER_EXCHANGE);
    }

    @Bean
    public Queue orderQueue() {

        Map<String, Object> arguments = new HashMap<>();

        arguments.put("x-dead-letter-exchange", ORDER_DLX);
        arguments.put("x-dead-letter-routing-key", ORDER_DLQ);

        return new Queue(
                ORDER_QUEUE,
                true,
                false,
                false,
                arguments
        );
    }

    @Bean
    public DirectExchange orderDeadLetterExchange(){
        return new DirectExchange(ORDER_DLX);
    }

    @Bean
    public Queue orderDeadLetterQueue() {
        return new Queue(ORDER_DLQ, true);
    }

    @Bean
    public Binding orderBinding(Queue orderQueue, DirectExchange orderExchange) {
        return BindingBuilder
                .bind(orderQueue)
                .to(orderExchange)
                .with(ORDER_ROUTING_KEY);
    }

    @Bean
    public Binding orderDeadLetterBinding(Queue orderDeadLetterQueue, DirectExchange orderDeadLetterExchange) {
        return BindingBuilder
                .bind(orderDeadLetterQueue)
                .to(orderDeadLetterExchange)
                .with("order.dlq");
    }

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory) {

        SimpleRabbitListenerContainerFactory factory =
                new SimpleRabbitListenerContainerFactory();

        factory.setConnectionFactory(connectionFactory);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);

        return factory;
    }
}