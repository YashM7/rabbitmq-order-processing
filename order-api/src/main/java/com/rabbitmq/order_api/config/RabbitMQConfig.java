package com.rabbitmq.order_api.config;


import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class RabbitMQConfig {

    public static final String ORDER_EXCHANGE = "order.exchange";
    public static final String ORDER_ROUTING_KEY = "order.created";
//    public static final String ORDER_QUEUE = "order.queue";
//    public static final String ORDER_DLX = "order.dlx";
//    public static final String ORDER_DLQ = "order.dlq";
//    public static final String ORDER_RETRY_QUEUE = "order.retry";
//    public static final String ORDER_RETRY_ROUTING_KEY = "order.retry";

//    @Bean
//    public DirectExchange orderExchange() {
//        return new DirectExchange(ORDER_EXCHANGE);
//    }
//
//    @Bean
//    public Queue orderQueue() {
//
//        Map<String, Object> arguments = new HashMap<>();
//
//        arguments.put("x-dead-letter-exchange", ORDER_DLX);
//        arguments.put("x-dead-letter-routing-key", ORDER_DLQ);
//
//        return new Queue(
//                ORDER_QUEUE,
//                true,
//                false,
//                false,
//                arguments
//        );
//    }
//
//    @Bean
//    public Queue orderRetryQueue() {
//
//        Map<String, Object> arguments = new HashMap<>();
//
//        arguments.put("x-dead-letter-exchange", ORDER_EXCHANGE);
//        arguments.put("x-dead-letter-routing-key", ORDER_ROUTING_KEY);
//
//        return new Queue(
//                ORDER_RETRY_QUEUE,
//                true,
//                false,
//                false,
//                arguments
//        );
//    }
//
//    @Bean
//    public DirectExchange orderDeadLetterExchange(){
//        return new DirectExchange(ORDER_DLX);
//    }
//
//    @Bean
//    public Queue orderDeadLetterQueue() {
//        return new Queue(ORDER_DLQ, true);
//    }
//
//    @Bean
//    public Binding orderBinding(Queue orderQueue, DirectExchange orderExchange) {
//        return BindingBuilder
//                .bind(orderQueue)
//                .to(orderExchange)
//                .with(ORDER_ROUTING_KEY);
//    }
//
//    @Bean
//    public Binding orderDeadLetterBinding(Queue orderDeadLetterQueue, DirectExchange orderDeadLetterExchange) {
//        return BindingBuilder
//                .bind(orderDeadLetterQueue)
//                .to(orderDeadLetterExchange)
//                .with("order.dlq");
//    }
//
//    @Bean
//    public Binding orderRetryBinding(Queue orderRetryQueue, DirectExchange orderExchange) {
//        return BindingBuilder
//                .bind(orderRetryQueue)
//                .to(orderExchange)
//                .with(ORDER_RETRY_ROUTING_KEY);
//    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new JacksonJsonMessageConverter();
    }
}