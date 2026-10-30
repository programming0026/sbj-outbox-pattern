package com.juniverse.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.juniverse.outbox.consumer.Notification;
import com.juniverse.outbox.consumer.NotificationRepository;
import com.juniverse.outbox.order.CreateOrderRequest;
import com.juniverse.outbox.order.Order;
import com.juniverse.outbox.order.OrderRepository;
import com.juniverse.outbox.order.OrderService;
import com.juniverse.outbox.outbox.OutboxRecorder;
import com.juniverse.outbox.outbox.OutboxRepository;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.context.WebApplicationContext;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Flujo completo con PostgreSQL y Kafka reales (Testcontainers; requiere Docker): pedido -> outbox -> relay -> Kafka -> consumidor.
 */
@SpringBootTest(properties = "outbox.relay-delay-ms=200")
@Testcontainers(disabledWithoutDocker = true)
@DisplayName("Transactional Outbox: flujo completo")
class OutboxFlowIT {

    @Container
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16-alpine");
    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.0.0");

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired private WebApplicationContext context;
    @Autowired private OrderService orderService;
    @Autowired private OrderRepository orderRepository;
    @Autowired private OutboxRepository outboxRepository;
    @Autowired private OutboxRecorder outboxRecorder;
    @Autowired private NotificationRepository notificationRepository;
    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private PlatformTransactionManager transactionManager;
    @Autowired private JdbcTemplate jdbc;

    private List<Notification> awaitNotifications(String orderId, int expected) {
        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> notificationRepository.findByAggregateIdOrderById(orderId).size() >= expected);
        return notificationRepository.findByAggregateIdOrderById(orderId);
    }

    @Test
    @DisplayName("el pedido genera un evento que el relay publica y el consumidor procesa")
    void eventTravelsFromDatabaseToConsumer() {
        Order order = orderService.create(new CreateOrderRequest("ana", new BigDecimal("150.00")));
        String id = order.getId().toString();

        Long pendingForOrder = jdbc.queryForObject("select count(*) from outbox_events where aggregate_id = ?", Long.class, id);
        assertEquals(1, pendingForOrder, "el evento se guardó junto con el pedido");

        List<Notification> notifications = awaitNotifications(id, 1);
        assertEquals("OrderCreated", notifications.get(0).getEventType());
        assertTrue(notifications.get(0).getPayload().contains("\"customer\":\"ana\""));

        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                jdbc.queryForObject("select count(*) from outbox_events where aggregate_id = ? and published_at is not null", Long.class, id) == 1);
    }

    @Test
    @DisplayName("ATOMICIDAD: si la transacción se deshace no queda ni pedido ni evento")
    void rollbackLeavesNothing() {
        long ordersBefore = orderRepository.count();
        long eventsBefore = outboxRepository.count();

        TransactionTemplate tx = new TransactionTemplate(transactionManager);
        assertThrows(IllegalStateException.class, () -> tx.executeWithoutResult(status -> {
            orderService.create(new CreateOrderRequest("fallido", new BigDecimal("10.00")));
            throw new IllegalStateException("falla de negocio posterior al guardado");
        }));

        assertEquals(ordersBefore, orderRepository.count());
        assertEquals(eventsBefore, outboxRepository.count());
    }

    @Test
    @DisplayName("el outbox no se puede usar fuera de una transacción")
    void recorderRequiresTransaction() {
        assertThrows(IllegalTransactionStateException.class, () -> outboxRecorder.record("Order", "x", "Algo", java.util.Map.of("a", 1)));
    }

    @Test
    @DisplayName("ORDEN: los eventos de un mismo pedido llegan en el orden en que ocurrieron")
    void eventsOfSameOrderKeepOrder() {
        Order order = orderService.create(new CreateOrderRequest("luis", new BigDecimal("20.00")));
        orderService.cancel(order.getId());

        List<Notification> notifications = awaitNotifications(order.getId().toString(), 2);
        assertEquals(List.of("OrderCreated", "OrderCancelled"), notifications.stream().map(Notification::getEventType).toList());
    }

    @Test
    @DisplayName("IDEMPOTENCIA: un evento entregado dos veces se procesa una sola vez")
    void duplicateDeliveryIsIgnored() throws Exception {
        String aggregate = UUID.randomUUID().toString();
        UUID eventId = UUID.randomUUID();
        kafkaTemplate.send(record(aggregate, eventId, "OrderCreated")).get(10, TimeUnit.SECONDS);
        kafkaTemplate.send(record(aggregate, eventId, "OrderCreated")).get(10, TimeUnit.SECONDS); // mismo event-id: duplicado
        kafkaTemplate.send(record(aggregate, UUID.randomUUID(), "OrderCancelled")).get(10, TimeUnit.SECONDS); // centinela

        List<Notification> notifications = awaitNotifications(aggregate, 2);
        assertEquals(2, notifications.size(), "el duplicado no generó una segunda notificación");
        assertEquals(List.of("OrderCreated", "OrderCancelled"), notifications.stream().map(Notification::getEventType).toList());
    }

    private static ProducerRecord<String, String> record(String key, UUID eventId, String type) {
        ProducerRecord<String, String> record = new ProducerRecord<>("orders.events", key, "{}");
        record.headers().add("event-id", eventId.toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("event-type", type.getBytes(StandardCharsets.UTF_8));
        return record;
    }

    @Test
    @DisplayName("API: crear (201 + Location), validar (400), cancelar (200) y cancelar otra vez (409)")
    void api() throws Exception {
        MockMvc mvc = MockMvcBuilders.webAppContextSetup(context).build();
        String body = mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content("{\"customer\":\"eva\",\"amount\":99.90}"))
                .andExpect(status().isCreated())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.status").value("CREATED"))
                .andReturn().getResponse().getContentAsString();
        String id = body.replaceAll(".*\"id\":\"([0-9a-f-]{36})\".*", "$1");

        mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content("{\"customer\":\"\",\"amount\":0}")).andExpect(status().isBadRequest());
        mvc.perform(post("/api/orders/" + id + "/cancel")).andExpect(status().isOk()).andExpect(jsonPath("$.status").value("CANCELLED"));
        mvc.perform(post("/api/orders/" + id + "/cancel")).andExpect(status().isConflict());
        mvc.perform(get("/api/orders/" + UUID.randomUUID())).andExpect(status().isNotFound());

        awaitNotifications(id, 2);
        mvc.perform(get("/api/notifications/order/" + id)).andExpect(jsonPath("$.length()").value(2)).andExpect(jsonPath("$[0].eventType").value("OrderCreated"));
        mvc.perform(get("/api/outbox/stats")).andExpect(status().isOk()).andExpect(jsonPath("$.dead").value(0));
    }
}
