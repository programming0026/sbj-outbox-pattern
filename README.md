# Transactional Outbox con Spring Boot 4, PostgreSQL y Kafka

Ejemplo con **Spring Boot 4** y **Java 21** del patrón **Transactional Outbox**: cómo cambiar la base de datos Y publicar un evento en Kafka sin que uno de los dos pueda quedar a medias.

## El problema: la "doble escritura"

Al crear un pedido hay que (1) guardarlo en PostgreSQL y (2) avisar por Kafka. Son dos sistemas y no existe una transacción que abarque ambos:

```text
guardar pedido  ──► OK
publicar evento ──► ✗ Kafka no responde      → el pedido existe pero nadie se entera
                                               (inventario, facturación y envíos nunca lo ven)

publicar evento ──► OK
guardar pedido  ──► ✗ la base falla          → se anuncia un pedido que no existe
```

Ni el orden ni un `try/catch` lo arreglan: siempre hay una ventana donde un fallo deja los dos sistemas inconsistentes.

## La solución

```text
┌──────────────── UNA transacción de PostgreSQL ────────────────┐
│  INSERT INTO orders ...            (el cambio de negocio)      │
│  INSERT INTO outbox_events ...     (el evento, como una fila)  │
└────────────────────────────────────────────────────────────────┘
        o se guardan los dos, o ninguno
                         │
                         ▼
  Relay (proceso aparte): lee outbox_events pendientes ──► Kafka ──► marca published_at
                                                              │
                                                              ▼
                                         Consumidor (idempotente) ──► efecto
```

1. El servicio escribe el pedido **y** el evento en la misma transacción local. Eso sí es atómico.
2. Un **relay** lee periódicamente los eventos pendientes y los publica en Kafka; al confirmarlos los marca como publicados.
3. Si Kafka está caído, los eventos esperan en la tabla y se envían cuando vuelva. No se pierde nada.

## Garantías y compromisos

| Tema | Qué ofrece este ejemplo |
|------|-------------------------|
| Atomicidad | Pedido y evento se guardan juntos o no se guarda nada (prueba con *rollback*) |
| Entrega | **Al menos una vez** (*at-least-once*): si la app cae tras enviar a Kafka pero antes del commit, el evento se reenvía |
| Duplicados | El consumidor es **idempotente**: anota el `event-id` de cada evento procesado en la misma transacción que su efecto (tabla `processed_messages`, el "inbox") |
| Orden | La clave del mensaje es el id del pedido: Kafka conserva el orden dentro de cada pedido. Si un envío falla, el relay detiene la vuelta para no adelantar eventos posteriores |
| Varias instancias | `SELECT ... FOR UPDATE SKIP LOCKED`: cada instancia toma eventos distintos sin esperarse ni duplicar envíos |
| Fallos permanentes | Tras `outbox.max-attempts` intentos el evento queda "muerto": no bloquea la cola, sigue en la tabla y aparece en `/api/outbox/stats` |
| Crecimiento | Limpieza periódica de los eventos publicados (`outbox.retention`) |

> "Exactamente una vez" no existe de extremo a extremo: lo que se consigue es *al menos una vez + consumidor idempotente*.

## Estructura

```text
src/main/java/com/juniverse/outbox/
├── OutboxApplication.java
├── controller/                # OrderController, OutboxController, NotificationController
├── service/
│   ├── OrderService.java      # el negocio
│   ├── OutboxRecorder.java    # escribe el evento; exige transacción abierta (Propagation.MANDATORY)
│   ├── OutboxRelay.java       # publica en Kafka y marca como enviado
│   ├── OutboxScheduler.java   # dispara el relay y la limpieza
│   └── NotificationService.java  # consumidor idempotente
├── messaging/                 # NotificationConsumer
├── repository/                # OrderRepository, OutboxRepository (consulta FOR UPDATE SKIP LOCKED), NotificationRepository, ProcessedMessageRepository
├── model/                     # Order, OrderStatus, OutboxEvent (fila de outbox_events), Notification, ProcessedMessage
├── dto/                       # CreateOrderRequest, OrderResponse, NotificationView, OrderEventPayload (contenido del evento)
├── exception/                 # OrderNotFoundException, OrderAlreadyCancelledException
└── config/                    # KafkaTopicConfig, OutboxProperties
```

Detalles que conviene notar:

- `OrderService` **no usa `KafkaTemplate`**: solo toca la base de datos. Quien publica es el relay.
- `OutboxRecorder` usa `Propagation.MANDATORY`: si alguien lo llama sin transacción, falla. Así no se puede guardar un evento fuera de la transacción del cambio.
- El índice `idx_outbox_pending` es **parcial** (`where published_at is null`): solo contiene los pendientes, así que el relay es rápido aunque la tabla acumule millones de eventos publicados.

## Requisitos

- JDK 21, Maven 3.9 o superior y Docker.

## Ejecutar

```bash
docker compose up -d        # PostgreSQL 16 y Kafka 4 (KRaft, un solo nodo)
mvn spring-boot:run
```

```bash
# Crear un pedido: guarda el pedido y el evento en la misma transacción
curl -X POST http://localhost:8080/api/orders -H "Content-Type: application/json" \
     -d '{"customer":"ana","amount":120.50}'

# Cancelarlo: segundo evento (OrderCancelled) del mismo pedido
curl -X POST http://localhost:8080/api/orders/<id>/cancel

# Lo que recibió el consumidor, en orden
curl http://localhost:8080/api/notifications/order/<id>

# Estado del outbox: pendientes y muertos
curl http://localhost:8080/api/outbox/stats
```

**Experimento:** detén Kafka (`docker compose stop kafka`), crea varios pedidos y mira `/api/outbox/stats`: `pending` crece pero los pedidos se crearon bien. Vuelve a levantarlo (`docker compose start kafka`) y los eventos se publican solos, en orden. Con `docker exec -it outbox-postgres psql -U outbox -c "select event_type, published_at, attempts from outbox_events"` ves la tabla.

## Pruebas

```bash
mvn verify
```

10 pruebas de integración con Testcontainers (PostgreSQL y Kafka reales; requieren Docker):

- `OutboxFlowIT` (6): el evento viaja de la base al consumidor; **rollback** sin pedido ni evento; el outbox exige transacción; **orden** de eventos de un mismo pedido; **duplicados** descartados; y la API (201, 400, 409, 404).
- `OutboxRelayFailureIT` (4, con Kafka simulado): reintentos hasta lograr publicar, evento "muerto" tras agotar los intentos, un fallo detiene la vuelta para conservar el orden, y limpieza de eventos antiguos.

## Alternativas y límites

- **Polling** (este ejemplo): simple y portable; añade la latencia del intervalo (`outbox.relay-delay-ms`) y carga de lectura.
- **CDC con Debezium**: lee el log de transacciones de PostgreSQL y publica en Kafka sin consultas periódicas; menos latencia, más infraestructura.
- Con mucho volumen, particiona o archiva la tabla y ajusta `outbox.batch-size`.
- Los eventos son JSON en texto; en producción considera Avro/Protobuf con Schema Registry y versiona los eventos.
- No lo uses entre dos tablas de la misma base: ahí basta una transacción normal.

Fat jar ejecutable: `mvn clean package` y `java -jar target/sbj-outbox-pattern-0.0.1-SNAPSHOT.jar`.

## Licencia

Código de ejemplo de uso libre con fines educativos.
