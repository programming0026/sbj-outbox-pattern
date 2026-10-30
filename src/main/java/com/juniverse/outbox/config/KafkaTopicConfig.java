package com.juniverse.outbox.config;

import com.juniverse.outbox.outbox.OutboxProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Crea el tema de eventos al arrancar (si no existe). */
@Configuration
public class KafkaTopicConfig {

    /**
     * Tema con 3 particiones: los mensajes con la misma clave (mismo pedido) van siempre a la misma partición, lo que
     * conserva su orden mientras se reparte la carga entre pedidos.
     *
     * @param properties propiedades del outbox
     * @return definición del tema
     */
    @Bean
    public NewTopic ordersTopic(OutboxProperties properties) {
        return TopicBuilder.name(properties.topic()).partitions(3).replicas(1).build();
    }
}
