package com.rabbitmq.order_worker.service;

import com.rabbitmq.order_worker.entity.Order;
import com.rabbitmq.order_worker.repository.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Service
public class OrderProcessor {

    private final OrderRepository orderRepository;
    private final JsonMapper jsonMapper;

    public OrderProcessor(
            OrderRepository orderRepository,
            JsonMapper jsonMapper) {

        this.orderRepository = orderRepository;
        this.jsonMapper = jsonMapper;
    }

    @Transactional
    public void process(String orderJson) throws Exception {

        System.out.println("Processing order: " + orderJson);
        Order order = jsonMapper.readValue(orderJson, Order.class);

        if (orderRepository.existsById(order.getOrderId())) {
            System.out.println(
                    "Order already exists, skipping: " + order.getOrderId()
            );
            return;
        }

        orderRepository.save(order);
        System.out.println(
                "Order saved to database: " + order.getOrderId()
        );
    }
}