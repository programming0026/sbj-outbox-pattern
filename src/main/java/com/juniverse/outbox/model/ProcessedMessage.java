package com.juniverse.outbox.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/** Registro de los eventos ya procesados (tabla "inbox"): permite descartar duplicados. */
@Entity
@Table(name = "processed_messages")
public class ProcessedMessage {

    @Id
    @Column(name = "event_id")
    private UUID eventId;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    /** Constructor requerido por JPA. */
    protected ProcessedMessage() {
    }

    /**
     * Crea el registro.
     *
     * @param eventId id del evento procesado
     */
    public ProcessedMessage(UUID eventId) {
        this.eventId = eventId;
        this.processedAt = Instant.now();
    }

    public UUID getEventId() { return eventId; }

    public Instant getProcessedAt() { return processedAt; }
}
