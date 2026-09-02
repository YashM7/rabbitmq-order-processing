package com.rabbitmq.order_api.controller;

import com.rabbitmq.order_api.DTO.OrderRequest;
import com.rabbitmq.order_api.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<String> createOrder(@Valid @RequestBody OrderRequest order) {

        orderService.createOrder(order);

        return ResponseEntity.ok("Order sent to RabbitMQ");
    }
}