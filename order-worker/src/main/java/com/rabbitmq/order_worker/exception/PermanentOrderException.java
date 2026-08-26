package com.rabbitmq.order_worker.exception;

public class PermanentOrderException extends OrderProcessingException {

    public PermanentOrderException(String message) {
        super(message);
    }

    public PermanentOrderException(String message, Throwable cause) {
        super(message, cause);
    }
}
