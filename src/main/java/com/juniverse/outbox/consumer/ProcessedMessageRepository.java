package com.juniverse.outbox.consumer;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

/** Acceso a los eventos ya procesados. */
public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, UUID> {
}
