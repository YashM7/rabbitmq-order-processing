package com.rabbitmq.order_worker.service;

import com.rabbitmq.order_worker.entity.Order;
import com.rabbitmq.order_worker.entity.OrderStatus;
import com.rabbitmq.order_worker.exception.DuplicateOrderException;
import com.rabbitmq.order_worker.exception.InvalidOrderException;
import com.rabbitmq.order_worker.exception.PermanentOrderException;
import com.rabbitmq.order_worker.exception.TransientOrderException;
import com.rabbitmq.order_worker.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;

@Service
public class OrderProcessor {

    private final OrderRepository orderRepository;
    private static final Logger log = LoggerFactory.getLogger(OrderProcessor.class);

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

        if(order.getProduct() == null || order.getProduct().isBlank()) {
            throw new InvalidOrderException("Product is required");
        }

        try {
            if (orderRepository.existsById(order.getOrderId())) {
                System.out.println("Order already exists, skipping: " + order.getOrderId());
                return;
            }

            order.setStatus(OrderStatus.COMPLETED);
            orderRepository.saveAndFlush(order);
            System.out.println("Order saved to database: " + order.getOrderId());

        } catch (DataIntegrityViolationException e) {
            Throwable cause = e.getCause();

            if(cause instanceof org.hibernate.exception.ConstraintViolationException constraintViolation
                && "orders_pkey".equals(constraintViolation.getConstraintName())) {

                throw new DuplicateOrderException(
                        "Duplicate order detected (race condition) for " + order.getOrderId(), e);
            }

            throw new PermanentOrderException("Data integrity violation for order " + order.getOrderId(), e);

        } catch (DataAccessException e) {
            throw new TransientOrderException(
                    "Transient DB failure for order " + order.getOrderId(),
                    e
            );
        }
    }

    public void processWithUpsert(Order order) {

        Instant start = Instant.now();

        try {

            log.info("Processing order: " + order);

            if (order.getOrderId() == null || order.getOrderId().isBlank()) {
                throw new InvalidOrderException("Order ID is required");
            }

            if (order.getQuantity() <= 0) {
                throw new InvalidOrderException("Quantity must be greater than zero");
            }

            if (order.getProduct() == null || order.getProduct().isBlank()) {
                throw new InvalidOrderException("Product is required");
            }

            try {
                int rowsAffected = orderRepository.insertIfNotExists(
                        order.getOrderId(),
                        order.getProduct(),
                        order.getQuantity(),
                        OrderStatus.COMPLETED.name()
                );

                if (rowsAffected == 1) {
                    log.info("Order inserted: " + order.getOrderId());
                } else {
                    log.warn("Order already exists, skipped (ON CONFLICT): " + order.getOrderId());
                }

            } catch (DataAccessException e) {
                throw new TransientOrderException(
                        "Transient DB failure for order " + order.getOrderId(), e);
            }

        } finally {
            long durationMs = Duration.between(start, Instant.now()).toMillis();
            log.info("Processing duration: " + durationMs + "ms");
        }
    }
}