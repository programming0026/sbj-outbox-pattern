package com.juniverse.outbox.dto;

import com.juniverse.outbox.model.Order;
import com.juniverse.outbox.model.OrderStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Pedido tal como lo ve el cliente.
 *
 * @param id identificador
 * @param customer cliente
 * @param amount importe
 * @param status estado
 * @param createdAt fecha de alta
 */
public record OrderResponse(UUID id, String customer, BigDecimal amount, OrderStatus status, Instant createdAt) {

    public static OrderResponse from(Order order) {
        return new OrderResponse(order.getId(), order.getCustomer(), order.getAmount(), order.getStatus(), order.getCreatedAt());
    }
}
