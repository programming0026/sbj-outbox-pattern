package com.juniverse.outbox.controller;

import com.juniverse.outbox.dto.NotificationView;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.juniverse.outbox.repository.NotificationRepository;

/** Consulta de las notificaciones que generó el consumidor. */
@RestController
@RequestMapping("/api/notifications")
public class NotificationController {

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
