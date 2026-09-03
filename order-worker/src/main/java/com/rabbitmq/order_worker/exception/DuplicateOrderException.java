package com.rabbitmq.order_worker.exception;

public class DuplicateOrderException extends OrderProcessingException {

    public DuplicateOrderException(String message) {
        super(message);
    }

    public DuplicateOrderException(String message, Throwable cause) {
        super(message, cause);
    }
}
