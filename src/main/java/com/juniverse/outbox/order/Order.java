package com.juniverse.outbox.order;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Pedido: el agregado de negocio. Su tabla se escribe en la misma transacción que el evento del outbox. */
@Entity
@Table(name = "orders")
public class Order {

    @Id
    private UUID id;

    @Column(nullable = false, length = 100)
    private String customer;

    @Column(nullable = false, precision = 12, scale = 2)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private OrderStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    /** Constructor requerido por JPA. */
    protected Order() {
    }

    /**
     * Crea un pedido nuevo en estado {@link OrderStatus#CREATED}.
     *
     * @param customer cliente
     * @param amount importe
     */
    public Order(String customer, BigDecimal amount) {
        this.id = UUID.randomUUID();
        this.customer = customer;
        this.amount = amount;
        this.status = OrderStatus.CREATED;
        this.createdAt = Instant.now();
    }

    /** Pasa el pedido a cancelado. */
    public void cancel() {
        this.status = OrderStatus.CANCELLED;
    }

    public UUID getId() { return id; }

    public String getCustomer() { return customer; }

    public BigDecimal getAmount() { return amount; }

    public OrderStatus getStatus() { return status; }

    public Instant getCreatedAt() { return createdAt; }
}
