package com.rabbitmq.order_worker.repository;

import com.rabbitmq.order_worker.entity.Order;
import com.rabbitmq.order_worker.entity.OrderStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
class OrderRepositoryTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16");

    @Autowired
    private OrderRepository orderRepository;

    @Test
    void shouldInsertOrder() {

        int rowsAffected = orderRepository.insertIfNotExists(
                "ORDER-TEST-1",
                "Laptop",
                2,
                OrderStatus.COMPLETED.name()
        );

        assertThat(rowsAffected).isEqualTo(1);

        Order savedOrder = orderRepository.findById("ORDER-TEST-1")
                .orElseThrow();

        assertThat(savedOrder.getOrderId()).isEqualTo("ORDER-TEST-1");
        assertThat(savedOrder.getProduct()).isEqualTo("Laptop");
        assertThat(savedOrder.getQuantity()).isEqualTo(2);
        assertThat(savedOrder.getStatus()).isEqualTo(OrderStatus.COMPLETED);
        assertThat(savedOrder.getCreatedAt()).isNotNull();
        assertThat(savedOrder.getUpdatedAt()).isNotNull();
    }

    @Test
    void shouldIgnoreDuplicateOrder() {

        int firstInsert = orderRepository.insertIfNotExists(
                "ORDER-DUPLICATE",
                "Laptop",
                2,
                OrderStatus.COMPLETED.name()
        );

        int secondInsert = orderRepository.insertIfNotExists(
                "ORDER-DUPLICATE",
                "Phone",
                99,
                OrderStatus.COMPLETED.name()
        );

        assertThat(firstInsert).isEqualTo(1);
        assertThat(secondInsert).isEqualTo(0);

        Order savedOrder = orderRepository.findById("ORDER-DUPLICATE")
                .orElseThrow();

        assertThat(savedOrder.getProduct()).isEqualTo("Laptop");
        assertThat(savedOrder.getQuantity()).isEqualTo(2);

        assertThat(orderRepository.findAll())
                .filteredOn(order -> "ORDER-DUPLICATE".equals(order.getOrderId()))
                .hasSize(1);
    }

    @Test
    void shouldReturnZeroWhenOrderAlreadyExists() {

        orderRepository.insertIfNotExists(
                "ORDER-EXISTING",
                "Laptop",
                1,
                OrderStatus.COMPLETED.name()
        );

        int rowsAffected = orderRepository.insertIfNotExists(
                "ORDER-EXISTING",
                "Laptop",
                1,
                OrderStatus.COMPLETED.name()
        );

        assertThat(rowsAffected).isZero();
    }

    @Test
    void shouldPersistDifferentOrdersIndependently() {

        int firstInsert = orderRepository.insertIfNotExists(
                "ORDER-1",
                "Laptop",
                1,
                OrderStatus.COMPLETED.name()
        );

        int secondInsert = orderRepository.insertIfNotExists(
                "ORDER-2",
                "Phone",
                3,
                OrderStatus.COMPLETED.name()
        );

        assertThat(firstInsert).isEqualTo(1);
        assertThat(secondInsert).isEqualTo(1);

        Order firstOrder = orderRepository.findById("ORDER-1")
                .orElseThrow();

        Order secondOrder = orderRepository.findById("ORDER-2")
                .orElseThrow();

        assertThat(firstOrder.getProduct()).isEqualTo("Laptop");
        assertThat(firstOrder.getQuantity()).isEqualTo(1);

        assertThat(secondOrder.getProduct()).isEqualTo("Phone");
        assertThat(secondOrder.getQuantity()).isEqualTo(3);

        assertThat(orderRepository.findAll())
                .filteredOn(order ->
                        order.getOrderId().equals("ORDER-1")
                                || order.getOrderId().equals("ORDER-2"))
                .hasSize(2);
    }

    @Test
    void shouldPersistCorrectStatus() {

        int rowsAffected = orderRepository.insertIfNotExists(
                "ORDER-STATUS",
                "Laptop",
                5,
                OrderStatus.PROCESSING.name()
        );

        assertThat(rowsAffected).isEqualTo(1);

        Order savedOrder = orderRepository.findById("ORDER-STATUS")
                .orElseThrow();

        assertThat(savedOrder.getStatus()).isEqualTo(OrderStatus.PROCESSING);
    }

    @Test
    void shouldPersistCreatedAndUpdatedTimestamps() {

        orderRepository.insertIfNotExists(
                "ORDER-TIMESTAMP",
                "Laptop",
                1,
                OrderStatus.COMPLETED.name()
        );

        Order savedOrder = orderRepository.findById("ORDER-TIMESTAMP")
                .orElseThrow();

        assertThat(savedOrder.getCreatedAt()).isNotNull();
        assertThat(savedOrder.getUpdatedAt()).isNotNull();
    }

    @Test
    void shouldNotOverwriteExistingOrderOnDuplicate() {

        orderRepository.insertIfNotExists(
                "ORDER-NO-OVERWRITE",
                "Laptop",
                2,
                OrderStatus.COMPLETED.name()
        );

        orderRepository.insertIfNotExists(
                "ORDER-NO-OVERWRITE",
                "Gaming PC",
                50,
                OrderStatus.FAILED.name()
        );

        Order savedOrder = orderRepository.findById("ORDER-NO-OVERWRITE")
                .orElseThrow();

        assertThat(savedOrder.getProduct()).isEqualTo("Laptop");
        assertThat(savedOrder.getQuantity()).isEqualTo(2);
        assertThat(savedOrder.getStatus()).isEqualTo(OrderStatus.COMPLETED);
    }
}
