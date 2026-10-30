package com.juniverse.outbox.consumer;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

/** Acceso a las notificaciones. */
public interface NotificationRepository extends JpaRepository<Notification, Long> {

    /**
     * Notificaciones de un pedido en el orden en que se procesaron.
     *
     * @param aggregateId pedido
     * @return notificaciones
     */
    List<Notification> findByAggregateIdOrderById(String aggregateId);
}
