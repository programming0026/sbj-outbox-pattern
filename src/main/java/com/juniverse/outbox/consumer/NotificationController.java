package com.juniverse.outbox.consumer;

import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Consulta de las notificaciones que generó el consumidor. */
@RestController
@RequestMapping("/api/notifications")
public class NotificationController {

    /**
     * Notificación para mostrar.
     *
     * @param eventId id del evento
     * @param eventType tipo de evento
     * @param payload contenido recibido
     */
    public record NotificationView(java.util.UUID eventId, String eventType, String payload) {
    }

    private final NotificationRepository repository;

    /**
     * Crea el controlador.
     *
     * @param repository notificaciones
     */
    public NotificationController(NotificationRepository repository) {
        this.repository = repository;
    }

    /**
     * Notificaciones de un pedido, en orden de proceso.
     *
     * @param orderId pedido
     * @return notificaciones
     */
    @GetMapping("/order/{orderId}")
    public List<NotificationView> byOrder(@PathVariable String orderId) {
        return repository.findByAggregateIdOrderById(orderId).stream()
                .map(n -> new NotificationView(n.getEventId(), n.getEventType(), n.getPayload())).toList();
    }
}
