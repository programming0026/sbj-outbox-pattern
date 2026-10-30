package com.juniverse.outbox.order;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;

/**
 * Datos para crear un pedido.
 *
 * @param customer cliente
 * @param amount importe
 */
public record CreateOrderRequest(
        @NotBlank(message = "El cliente es obligatorio") @Size(max = 100, message = "El cliente no puede superar los 100 caracteres") String customer,
        @NotNull(message = "El importe es obligatorio") @DecimalMin(value = "0.01", message = "El importe debe ser mayor que cero") BigDecimal amount) {
}
