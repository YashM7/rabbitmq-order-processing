package com.rabbitmq.order_worker.service;

import com.rabbitmq.order_worker.entity.Order;
import com.rabbitmq.order_worker.exception.InvalidOrderException;
import com.rabbitmq.order_worker.exception.TransientOrderException;
import com.rabbitmq.order_worker.repository.OrderRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.RecoverableDataAccessException;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderProcessorTest {

    @Mock
    private OrderRepository orderRepository;

    @InjectMocks
    private OrderProcessor orderProcessor;

    @Test
    void shouldInsertValidOrder() {
        Order order = new Order("test-order", "Laptop", 1);

        when(orderRepository.insertIfNotExists(
                "test-order",
                "Laptop",
                1,
                "COMPLETED"
        )).thenReturn(1);

        orderProcessor.processWithUpsert(order);

        verify(orderRepository).insertIfNotExists("test-order", "Laptop", 1, "COMPLETED");
        verifyNoMoreInteractions(orderRepository);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "   "})
    void shouldRejectOrderWithInvalidOrderId(String orderId) {
        Order order = new Order(orderId, "Laptop", 1);

        assertThrows(
                InvalidOrderException.class,
                () -> orderProcessor.processWithUpsert(order)
        );

        verifyNoInteractions(orderRepository);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "   "})
    void shouldRejectOrderWithInvalidProduct(String invalidProduct) {
        Order order = new Order("test-order", invalidProduct, 1);

        assertThrows(
                InvalidOrderException.class,
                () -> orderProcessor.processWithUpsert(order)
        );

        verifyNoInteractions(orderRepository);
    }


    @ParameterizedTest
    @ValueSource(ints = {0, -1, -10})
    void shouldRejectInvalidQuantity(int quantity) {
        Order order = new Order("test-order", "Laptop", quantity);

        assertThrows(
                InvalidOrderException.class,
                () -> orderProcessor.processWithUpsert(order)
        );

        verifyNoInteractions(orderRepository);
    }

    @Test
    void shouldRejectNullOrder() {
        assertThrows(
                InvalidOrderException.class,
                () -> orderProcessor.processWithUpsert(null)
        );

        verifyNoInteractions(orderRepository);
    }

    @Test
    void shouldSkipExistingOrder() {
        Order order = new Order("test-order", "Laptop", 1);

        when(orderRepository.insertIfNotExists(
                "test-order",
                "Laptop",
                1,
                "COMPLETED"
        )).thenReturn(0);

        orderProcessor.processWithUpsert(order);

        verify(orderRepository).insertIfNotExists(
                "test-order",
                "Laptop",
                1,
                "COMPLETED"
        );
    }

    @Test
    void shouldTranslateDatabaseFailureToTransientOrderException() {
        Order order = new Order("test-order", "Laptop", 1);

        RecoverableDataAccessException databaseException =
                new RecoverableDataAccessException(
                        "Database is temporarily unavailable"
                );

        when(orderRepository.insertIfNotExists(
                "test-order",
                "Laptop",
                1,
                "COMPLETED"
        )).thenThrow(databaseException);

        // Act + Assert
        TransientOrderException exception = assertThrows(
                TransientOrderException.class,
                () -> orderProcessor.processWithUpsert(order)
        );

        // Verify that the original database exception was preserved
        assertSame(databaseException, exception.getCause());

        // Verify repository interaction
        verify(orderRepository).insertIfNotExists(
                "test-order",
                "Laptop",
                1,
                "COMPLETED"
        );
    }

    @Test
    void shouldPropagateUnexpectedException() {
        Order order = new Order("test-order", "Laptop", 1);

        RuntimeException unexpectedException =
                new RuntimeException("Unexpected failure");

        when(orderRepository.insertIfNotExists(
                "test-order",
                "Laptop",
                1,
                "COMPLETED"
        )).thenThrow(unexpectedException);

        // Act + Assert
        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> orderProcessor.processWithUpsert(order)
        );

        // Verify that the original exception was not replaced
        assertSame(unexpectedException, exception);

        // Verify repository interaction
        verify(orderRepository).insertIfNotExists(
                "test-order",
                "Laptop",
                1,
                "COMPLETED"
        );
    }
}