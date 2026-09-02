package com.rabbitmq.order_worker.entity;

public enum OrderStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    FAILED
}
