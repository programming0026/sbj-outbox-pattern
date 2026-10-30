package com.juniverse.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.juniverse.outbox.order.CreateOrderRequest;
import com.juniverse.outbox.order.Order;
import com.juniverse.outbox.order.OrderService;
import com.juniverse.outbox.outbox.OutboxRelay;
import com.juniverse.outbox.outbox.OutboxRepository;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Qué pasa cuando Kafka FALLA: el evento no se pierde, queda en el outbox y se reintenta. Solo PostgreSQL es real;
 * Kafka se simula con un mock que falla a voluntad.
 */
@SpringBootTest(properties = {
        "outbox.relay-delay-ms=100",
        "outbox.max-attempts=3",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false"})
@Testcontainers(disabledWithoutDocker = true)
@DisplayName("Transactional Outbox: fallos de Kafka")
class OutboxRelayFailureIT {

    @Container
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16-alpine");

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @MockitoBean
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired private OrderService orderService;
    @Autowired private OutboxRepository outboxRepository;
    @Autowired private OutboxRelay relay;
    @Autowired private JdbcTemplate jdbc;

    @BeforeEach
    void clean() {
        jdbc.update("delete from outbox_events");
    }

    private static CompletableFuture<SendResult<String, String>> failure() {
        return CompletableFuture.failedFuture(new IllegalStateException("broker caído"));
    }

    private static CompletableFuture<SendResult<String, String>> success() {
        return CompletableFuture.completedFuture(null);
    }

    @Test
    @DisplayName("si Kafka falla, el evento queda pendiente y se reintenta hasta publicarse")
    void retriesUntilPublished() {
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failure(), failure(), success());

        Order order = orderService.create(new CreateOrderRequest("ana", new BigDecimal("10.00")));
        String id = order.getId().toString();

        Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() ->
                jdbc.queryForObject("select count(*) from outbox_events where aggregate_id = ? and published_at is not null", Long.class, id) == 1);

        Map<String, Object> row = jdbc.queryForMap("select attempts, last_error from outbox_events where aggregate_id = ?", id);
        assertEquals(2, row.get("attempts"), "dos fallos antes del éxito");
        assertNotNull(row.get("last_error"));
        verify(kafkaTemplate, times(3)).send(any(ProducerRecord.class));
    }

    @Test
    @DisplayName("tras agotar los intentos el evento queda 'muerto': no se pierde y deja de reintentarse")
    void becomesDeadAfterMaxAttempts() throws Exception {
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failure());

        orderService.create(new CreateOrderRequest("luis", new BigDecimal("20.00")));

        Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() -> outboxRepository.countDead(3) == 1);
        Thread.sleep(600); // varias vueltas más del relay
        verify(kafkaTemplate, times(3)).send(any(ProducerRecord.class));
        assertEquals(1, outboxRepository.countByPublishedAtIsNull(), "sigue en la tabla para revisarlo o reenviarlo");
    }

    @Test
    @DisplayName("un fallo detiene la vuelta para no adelantar eventos posteriores")
    void failureStopsTheBatch() {
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failure());
        jdbc.update("insert into outbox_events (id, aggregate_type, aggregate_id, event_type, payload, created_at) values "
                + "(gen_random_uuid(), 'Order', 'o1', 'A', '{}', now() - interval '2 minutes'), "
                + "(gen_random_uuid(), 'Order', 'o1', 'B', '{}', now() - interval '1 minute')");

        int published = relay.publishPending();

        assertEquals(0, published);
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
        assertEquals(1, jdbc.queryForObject("select count(*) from outbox_events where attempts = 1", Long.class));
        assertNull(jdbc.queryForObject("select max(published_at) from outbox_events", java.time.Instant.class));
    }

    @Test
    @DisplayName("la limpieza borra solo los eventos publicados hace más de la retención")
    void purgeDeletesOldPublishedEvents() {
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failure());
        jdbc.update("insert into outbox_events (id, aggregate_type, aggregate_id, event_type, payload, created_at, published_at) values "
                + "(gen_random_uuid(), 'Order', 'viejo', 'A', '{}', now() - interval '10 days', now() - interval '10 days'), "
                + "(gen_random_uuid(), 'Order', 'reciente', 'A', '{}', now(), now())");
        jdbc.update("insert into outbox_events (id, aggregate_type, aggregate_id, event_type, payload, created_at, attempts) "
                + "values (gen_random_uuid(), 'Order', 'pendiente', 'A', '{}', now() - interval '10 days', 3)");

        assertEquals(1, relay.purgePublished(Duration.ofDays(7)));
        assertEquals(2, jdbc.queryForObject("select count(*) from outbox_events", Long.class), "quedan el reciente y el pendiente");
    }
}
