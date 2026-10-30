package com.juniverse.outbox.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * Fila de la tabla {@code outbox_events}: un evento pendiente de publicar (o ya publicado). Mientras
 * {@code published_at} sea nulo, el evento está pendiente.
 */
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    @Id
    private UUID id;

    /** Tipo de agregado (por ejemplo, "Order"). */
    @Column(name = "aggregate_type", nullable = false, length = 50)
    private String aggregateType;

    /** Identificador del agregado: se usa como CLAVE del mensaje en Kafka para mantener el orden por pedido. */
    @Column(name = "aggregate_id", nullable = false, length = 64)
    private String aggregateId;

    @Column(name = "event_type", nullable = false, length = 50)
    private String eventType;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "last_error", columnDefinition = "text")
    private String lastError;

    /** Constructor requerido por JPA. */
    protected OutboxEvent() {
    }

    /**
     * Crea un evento pendiente.
     *
     * @param aggregateType tipo de agregado
     * @param aggregateId identificador del agregado
     * @param eventType tipo de evento
     * @param payload contenido en JSON
     */
    public OutboxEvent(String aggregateType, String aggregateId, String eventType, String payload) {
        this.id = UUID.randomUUID();
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.createdAt = Instant.now();
    }

    /**
     * Marca el evento como publicado.
     *
     * @param when momento de la publicación
     */
    public void markPublished(Instant when) {
        this.publishedAt = when;
    }

    /**
     * Anota un intento fallido.
     *
     * @param error descripción del error
     */
    public void markFailed(String error) {
        this.attempts++;
        this.lastError = error;
    }

    public UUID getId() { return id; }

    public String getAggregateType() { return aggregateType; }

    public String getAggregateId() { return aggregateId; }

    public String getEventType() { return eventType; }

    public String getPayload() { return payload; }

    public Instant getCreatedAt() { return createdAt; }

    public Instant getPublishedAt() { return publishedAt; }

    public int getAttempts() { return attempts; }

    public String getLastError() { return lastError; }
}
