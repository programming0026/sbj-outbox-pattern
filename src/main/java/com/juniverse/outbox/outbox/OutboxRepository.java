package com.juniverse.outbox.outbox;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** Acceso a la tabla del outbox. */
public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * Toma y BLOQUEA los eventos pendientes más antiguos. {@code FOR UPDATE SKIP LOCKED} hace que, si hay varias instancias
     * del relay, cada una se lleve eventos distintos sin esperarse ni publicar el mismo dos veces. Debe ejecutarse dentro
     * de una transacción: los bloqueos duran hasta el commit.
     *
     * @param maxAttempts los eventos con tantos intentos o más se consideran "muertos" y se saltan
     * @param batch cantidad máxima a tomar
     * @return eventos pendientes, del más antiguo al más nuevo
     */
    @Query(value = "select * from outbox_events where published_at is null and attempts < :maxAttempts "
            + "order by created_at, id limit :batch for update skip locked", nativeQuery = true)
    List<OutboxEvent> lockPending(@Param("maxAttempts") int maxAttempts, @Param("batch") int batch);

    /**
     * Cantidad de eventos aún sin publicar.
     *
     * @return pendientes (incluye los muertos)
     */
    long countByPublishedAtIsNull();

    /**
     * Eventos que agotaron sus intentos.
     *
     * @param maxAttempts límite de intentos
     * @return cantidad de eventos muertos
     */
    @Query("select count(e) from OutboxEvent e where e.publishedAt is null and e.attempts >= :maxAttempts")
    long countDead(@Param("maxAttempts") int maxAttempts);

    /**
     * Borra los eventos publicados antes de una fecha.
     *
     * @param before fecha límite
     * @return filas borradas
     */
    @Modifying
    @Query("delete from OutboxEvent e where e.publishedAt is not null and e.publishedAt < :before")
    int deletePublishedBefore(@Param("before") Instant before);
}
