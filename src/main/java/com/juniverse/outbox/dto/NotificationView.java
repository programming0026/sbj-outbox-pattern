package com.juniverse.outbox.dto;

/**
 * Notificación para mostrar.
 *
 * @param eventId id del evento
 * @param eventType tipo de evento
 * @param payload contenido recibido
 */
public record NotificationView(java.util.UUID eventId, String eventType, String payload) {
}
