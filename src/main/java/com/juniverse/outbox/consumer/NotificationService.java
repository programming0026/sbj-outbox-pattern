package com.juniverse.outbox.consumer;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Procesa un evento de forma <b>idempotente</b>: como Kafka y el relay garantizan "al menos una vez", el mismo evento puede
 * llegar repetido. Se anota el id de cada evento procesado en la MISMA transacción que el efecto; si vuelve a llegar, se
 * descarta. Es el complemento del outbox (a veces llamado "inbox").
 */
@Service
public class NotificationService {

    private static final Logger LOG = LoggerFactory.getLogger(NotificationService.class);

    private final ProcessedMessageRepository processed;
    private final NotificationRepository notifications;

    /**
     * Crea el servicio.
     *
     * @param processed eventos ya procesados
     * @param notifications notificaciones generadas
     */
    public NotificationService(ProcessedMessageRepository processed, NotificationRepository notifications) {
        this.processed = processed;
        this.notifications = notifications;
    }

    /**
     * Procesa un evento una sola vez.
     *
     * @param eventId identificador único del evento (cabecera {@code event-id})
     * @param eventType tipo de evento
     * @param aggregateId pedido
     * @param payload contenido JSON
     * @return {@code true} si se procesó; {@code false} si era un duplicado
     */
    @Transactional
    public boolean handle(UUID eventId, String eventType, String aggregateId, String payload) {
        if (processed.existsById(eventId)) {
            LOG.info("Evento {} duplicado: se ignora", eventId);
            return false;
        }
        notifications.save(new Notification(eventId, eventType, aggregateId, payload));
        processed.save(new ProcessedMessage(eventId));
        LOG.info("Notificación generada por {} del pedido {}", eventType, aggregateId);
        return true;
    }
}
