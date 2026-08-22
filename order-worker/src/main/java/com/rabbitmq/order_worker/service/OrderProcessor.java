package com.rabbitmq.order_worker.service;

import com.rabbitmq.order_worker.entity.Order;
import com.rabbitmq.order_worker.exception.InvalidOrderException;
import com.rabbitmq.order_worker.repository.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderProcessor {

    private final OrderRepository orderRepository;

    public OrderProcessor(OrderRepository orderRepository) {

        this.orderRepository = orderRepository;
    }

    @Transactional
    public void process(Order order) {

        System.out.println("Processing order: " + order);

        if(order.getOrderId() == null || order.getOrderId().isBlank()) {
            throw new InvalidOrderException("Order ID is required");
        }

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