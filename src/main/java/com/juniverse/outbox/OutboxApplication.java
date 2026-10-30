package com.juniverse.outbox;

import com.juniverse.outbox.outbox.OutboxProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Patrón <b>Transactional Outbox</b> con PostgreSQL y Kafka.
 *
 * <p>Problema: al crear un pedido hay que guardarlo en la base de datos Y publicar un evento en Kafka. Son dos sistemas
 * distintos y no existe una transacción que abarque ambos ("dual write"): si se guarda y Kafka falla, nadie se entera del
 * pedido; si se publica y la base falla, se anuncia un pedido que no existe.</p>
 *
 * <p>Solución: el evento se guarda en una tabla {@code outbox_events} <b>en la misma transacción</b> que el pedido (o se
 * guardan los dos o ninguno), y un proceso aparte lee esa tabla y lo publica en Kafka, marcándolo como enviado.</p>
 */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(OutboxProperties.class)
public class OutboxApplication {

    /**
     * Arranca la aplicación.
     *
     * @param args argumentos de la línea de comandos
     */
    public static void main(String[] args) {
        SpringApplication.run(OutboxApplication.class, args);
    }
}
