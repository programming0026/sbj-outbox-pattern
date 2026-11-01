package com.juniverse.outbox.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Propiedades {@code outbox.*}.
 *
 * @param topic tema de Kafka donde se publican los eventos
 * @param batchSize máximo de eventos que el relay procesa en cada vuelta
 * @param maxAttempts intentos de publicación antes de dar un evento por "muerto" (requiere intervención manual)
 * @param relayDelayMs milisegundos entre una vuelta del relay y la siguiente
 * @param retention cuánto tiempo se conservan los eventos ya publicados antes de borrarlos
 */
@ConfigurationProperties(prefix = "outbox")
public record OutboxProperties(String topic, int batchSize, int maxAttempts, long relayDelayMs, Duration retention) {
}
