package com.juniverse.outbox.consumer;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Escucha el tema de eventos y delega el trabajo en {@link NotificationService}. */
@Component
public class NotificationConsumer {

    private final NotificationService service;

    /**
     * Crea el consumidor.
     *
     * @param service servicio idempotente
     */
    public NotificationConsumer(NotificationService service) {
        this.service = service;
    }

    /**
     * Recibe un mensaje de Kafka.
     *
     * @param record mensaje con clave (id del pedido), contenido JSON y cabeceras {@code event-id} y {@code event-type}
     */
    @KafkaListener(topics = "${outbox.topic}", groupId = "notifications")
    public void onMessage(ConsumerRecord<String, String> record) {
        UUID eventId = UUID.fromString(header(record, "event-id"));
        service.handle(eventId, header(record, "event-type"), record.key(), record.value());
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new IllegalArgumentException("Falta la cabecera " + name);
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
