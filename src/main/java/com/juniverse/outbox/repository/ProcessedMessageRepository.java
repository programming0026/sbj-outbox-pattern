package com.juniverse.outbox.repository;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import com.juniverse.outbox.model.ProcessedMessage;

/** Acceso a los eventos ya procesados. */
public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, UUID> {
}
