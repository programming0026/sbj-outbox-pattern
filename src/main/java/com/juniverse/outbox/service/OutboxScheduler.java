package com.juniverse.outbox.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.juniverse.outbox.config.OutboxProperties;

/** Dispara el relay y la limpieza periódicamente. */
@Component
public class OutboxScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxScheduler.class);

    private final OutboxRelay relay;
    private final OutboxProperties properties;

    /**
     * Crea el planificador.
     *
     * @param relay relay del outbox
     * @param properties propiedades del outbox
     */
    public OutboxScheduler(OutboxRelay relay, OutboxProperties properties) {
        this.relay = relay;
        this.properties = properties;
    }

    /** Publica los eventos pendientes cada {@code outbox.relay-delay-ms} milisegundos (la siguiente vuelta empieza cuando termina la anterior). */
    @Scheduled(fixedDelayString = "${outbox.relay-delay-ms}")
    public void relay() {
        try {
            int published = relay.publishPending();
            if (published > 0) {
                LOG.info("Outbox: {} evento(s) publicados en Kafka", published);
            }
        } catch (RuntimeException e) {
            LOG.error("Falló la vuelta del relay (se reintentará)", e);
        }
    }

    /** Borra cada hora los eventos publicados que superaron el tiempo de retención. */
    @Scheduled(fixedDelay = 3_600_000, initialDelay = 60_000)
    public void cleanup() {
        int deleted = relay.purgePublished(properties.retention());
        if (deleted > 0) {
            LOG.info("Outbox: {} evento(s) antiguos eliminados", deleted);
        }
    }
}
