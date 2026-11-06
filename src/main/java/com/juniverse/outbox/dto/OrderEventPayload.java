package com.juniverse.outbox.dto;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Contenido de los eventos de pedido.
 *
 * @param orderId pedido
 * @param customer cliente
 * @param amount importe
 * @param status estado tras el cambio
 */
public record OrderEventPayload(UUID orderId, String customer, BigDecimal amount, String status) {
}
