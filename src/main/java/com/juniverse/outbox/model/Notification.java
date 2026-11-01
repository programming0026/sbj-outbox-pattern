package com.juniverse.outbox.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/** Efecto del consumidor: una notificación generada a partir de un evento recibido. */
@Entity
@Table(name = "notifications")
public class Notification {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private UUID eventId;

    @Column(name = "event_type", nullable = false, length = 50)
    private String eventType;

    @Column(name = "aggregate_id", nullable = false, length = 64)
    private String aggregateId;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    /** Constructor requerido por JPA. */
    protected Notification() {
    }

    /**
     * Crea la notificación.
     *
     * @param eventId id del evento de origen
     * @param eventType tipo de evento
     * @param aggregateId pedido
     * @param payload contenido recibido
     */
    public Notification(UUID eventId, String eventType, String aggregateId, String payload) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.aggregateId = aggregateId;
        this.payload = payload;
        this.receivedAt = Instant.now();
    }

    public Long getId() { return id; }

    public UUID getEventId() { return eventId; }

    public String getEventType() { return eventType; }

    public String getAggregateId() { return aggregateId; }

    public String getPayload() { return payload; }

    public Instant getReceivedAt() { return receivedAt; }
}
