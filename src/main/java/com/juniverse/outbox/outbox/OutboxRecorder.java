package com.juniverse.outbox.outbox;

import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

/**
 * Registra eventos en el outbox. Usa {@link Propagation#MANDATORY}: si se llama SIN una transacción abierta lanza una
 * excepción. Así es imposible, por descuido, guardar un evento fuera de la transacción del cambio de negocio, que es justo
 * lo que el patrón quiere evitar.
 */
@Component
public class OutboxRecorder {

    private final OutboxRepository repository;
    private final JsonMapper jsonMapper;

    /**
     * Crea el registrador.
     *
     * @param repository tabla del outbox
     * @param jsonMapper convierte el contenido a JSON
     */
    public OutboxRecorder(OutboxRepository repository, JsonMapper jsonMapper) {
        this.repository = repository;
        this.jsonMapper = jsonMapper;
    }

    /**
     * Guarda un evento pendiente en la transacción actual.
     *
     * @param aggregateType tipo de agregado
     * @param aggregateId identificador del agregado (será la clave del mensaje de Kafka)
     * @param eventType tipo de evento
     * @param payload objeto que se serializa a JSON
     * @return identificador del evento
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public UUID record(String aggregateType, String aggregateId, String eventType, Object payload) {
        OutboxEvent event = new OutboxEvent(aggregateType, aggregateId, eventType, jsonMapper.writeValueAsString(payload));
        return repository.save(event).getId();
    }
}
