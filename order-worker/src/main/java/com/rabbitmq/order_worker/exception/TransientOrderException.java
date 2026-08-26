package com.rabbitmq.order_worker.exception;

public class TransientOrderException extends OrderProcessingException {

    public TransientOrderException(String message) {
        super(message);
    }

    public TransientOrderException(String message, Throwable cause) {
        super(message, cause);
    }
}
