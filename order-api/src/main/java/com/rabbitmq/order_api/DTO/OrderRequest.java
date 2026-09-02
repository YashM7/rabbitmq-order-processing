package com.rabbitmq.order_api.DTO;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record OrderRequest (

        @NotBlank(message = "Order ID is required")
        String orderId,

        @NotBlank(message = "Product is required")
        String product,

        @NotNull(message = "Quantity is required")
        @Positive(message = "Quantity must be greater than zero")
        Integer quantity
) {}