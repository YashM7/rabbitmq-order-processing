package com.rabbitmq.order_worker.service;

import com.rabbitmq.order_worker.entity.Order;
import com.rabbitmq.order_worker.exception.InvalidOrderException;
import com.rabbitmq.order_worker.exception.PermanentOrderException;
import com.rabbitmq.order_worker.exception.TransientOrderException;
import com.rabbitmq.order_worker.repository.OrderRepository;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
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

        if (order.getQuantity() <= 0) {
            throw new InvalidOrderException("Quantity must be greater than zero");
        }

        try {
            if (orderRepository.existsById(order.getOrderId())) {
                System.out.println("Order already exists, skipping: " + order.getOrderId());
                return;
            }

            orderRepository.save(order);
            System.out.println("Order saved to database: " + order.getOrderId());

        } catch (DataIntegrityViolationException e) {
            throw new PermanentOrderException("Data integrity violation for order " + order.getOrderId(), e);
        } catch (DataAccessException e) {
            throw new TransientOrderException(
                    "Transient DB failure for order " + order.getOrderId(),
                    e
            );
        }
    }
}