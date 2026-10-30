package com.juniverse.outbox.outbox;

import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Estado del outbox, útil para monitorear (un "pendientes" que crece indica que Kafka no está recibiendo). */
@RestController
@RequestMapping("/api/outbox")
public class OutboxController {

    private final OutboxRepository repository;
    private final OutboxProperties properties;

    /**
     * Crea el controlador.
     *
     * @param repository tabla del outbox
     * @param properties propiedades del outbox
     */
    public OutboxController(OutboxRepository repository, OutboxProperties properties) {
        this.repository = repository;
        this.properties = properties;
    }

    /**
     * Cuenta los eventos pendientes y los muertos.
     *
     * @return {@code pending} (sin publicar, incluye los muertos) y {@code dead} (agotaron sus intentos)
     */
    @GetMapping("/stats")
    public Map<String, Long> stats() {
        return Map.of("pending", repository.countByPublishedAtIsNull(), "dead", repository.countDead(properties.maxAttempts()));
    }
}
