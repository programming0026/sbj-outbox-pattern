package com.juniverse.outbox.outbox;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * El <b>relay</b>: lee los eventos pendientes del outbox y los publica en Kafka. Es la otra mitad del patrón.
 *
 * <p>Garantía: <b>al menos una vez</b> (at-least-once). Si la aplicación se cae después de enviar a Kafka pero antes de
 * confirmar la transacción, el evento seguirá pendiente y se enviará de nuevo: el consumidor debe tolerar duplicados
 * (ver {@code NotificationService}). Por eso cada mensaje lleva su {@code event-id}.</p>
 *
 * <p>Orden: se procesa por antigüedad y, si un envío falla, la vuelta se detiene para no adelantar eventos posteriores
 * del mismo pedido. La clave del mensaje es el id del agregado, así que Kafka mantiene el orden dentro de cada pedido.</p>
 */
@Component
public class OutboxRelay {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxRelay.class);
    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final OutboxRepository repository;
    private final KafkaTemplate<String, String> kafka;
    private final OutboxProperties properties;

    /**
     * Crea el relay.
     *
     * @param repository tabla del outbox
     * @param kafka plantilla de Kafka
     * @param properties propiedades del outbox
     */
    public OutboxRelay(OutboxRepository repository, KafkaTemplate<String, String> kafka, OutboxProperties properties) {
        this.repository = repository;
        this.kafka = kafka;
        this.properties = properties;
    }

    /**
     * Publica un lote de eventos pendientes. Toda la vuelta es una transacción: los eventos quedan bloqueados mientras se
     * envían y las marcas de "publicado" o "fallido" se confirman al final.
     *
     * @return cuántos eventos se publicaron en esta vuelta
     */
    @Transactional
    public int publishPending() {
        List<OutboxEvent> batch = repository.lockPending(properties.maxAttempts(), properties.batchSize());
        int published = 0;
        for (OutboxEvent event : batch) {
            try {
                send(event);
                event.markPublished(Instant.now());
                published++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                event.markFailed("interrumpido");
                break;
            } catch (ExecutionException | TimeoutException | RuntimeException e) {
                String reason = e.getClass().getSimpleName() + ": " + e.getMessage();
                event.markFailed(reason);
                LOG.warn("No se pudo publicar el evento {} (intento {}/{}): {}", event.getId(), event.getAttempts(), properties.maxAttempts(), reason);
                if (event.getAttempts() >= properties.maxAttempts()) {
                    LOG.error("El evento {} agotó sus intentos y requiere revisión manual", event.getId());
                }
                break; // se conserva el orden: no se publican eventos posteriores hasta resolver este
            }
        }
        return published;
    }

    private void send(OutboxEvent event) throws InterruptedException, ExecutionException, TimeoutException {
        ProducerRecord<String, String> record = new ProducerRecord<>(properties.topic(), event.getAggregateId(), event.getPayload());
        record.headers().add("event-id", event.getId().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("event-type", event.getEventType().getBytes(StandardCharsets.UTF_8));
        record.headers().add("aggregate-type", event.getAggregateType().getBytes(StandardCharsets.UTF_8));
        kafka.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS); // se espera la confirmación del broker
    }

    /**
     * Borra los eventos publicados hace más de {@code retention}: sin esta limpieza la tabla crece sin límite.
     *
     * @param retention antigüedad máxima que se conserva
     * @return filas borradas
     */
    @Transactional
    public int purgePublished(Duration retention) {
        return repository.deletePublishedBefore(Instant.now().minus(retention));
    }
}
