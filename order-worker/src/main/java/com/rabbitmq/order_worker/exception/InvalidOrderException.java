package com.rabbitmq.order_worker.exception;

public class InvalidOrderException extends PermanentOrderException {

    public InvalidOrderException(String message) {
        super(message);
    }
}