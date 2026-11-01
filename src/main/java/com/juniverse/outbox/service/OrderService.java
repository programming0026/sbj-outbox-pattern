package com.juniverse.outbox.service;

import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.juniverse.outbox.dto.CreateOrderRequest;
import com.juniverse.outbox.exception.OrderAlreadyCancelledException;
import com.juniverse.outbox.exception.OrderNotFoundException;
import com.juniverse.outbox.model.Order;
import com.juniverse.outbox.model.OrderStatus;
import com.juniverse.outbox.repository.OrderRepository;

/**
 * Casos de uso del pedido. Fíjate en que NO se usa {@code KafkaTemplate} aquí: el servicio solo escribe en la base de datos
 * (pedido + evento del outbox) dentro de UNA transacción. Publicar en Kafka es responsabilidad del relay, por separado.
 */
@Service
public class OrderService {

    private final OrderRepository orders;
    private final OutboxRecorder outbox;

    /**
     * Crea el servicio.
     *
     * @param orders repositorio de pedidos
     * @param outbox registrador de eventos del outbox
     */
    public OrderService(OrderRepository orders, OutboxRecorder outbox) {
        this.orders = orders;
        this.outbox = outbox;
    }

    /**
     * Crea un pedido y registra el evento {@code OrderCreated}. Si algo falla después, la transacción se deshace y no queda
     * ni el pedido ni el evento.
     *
     * @param request datos validados
     * @return el pedido creado
     */
    @Transactional
    public Order create(CreateOrderRequest request) {
        Order order = orders.save(new Order(request.customer().trim(), request.amount()));
        outbox.record("Order", order.getId().toString(), "OrderCreated", payload(order));
        return order;
    }

    /**
     * Cancela un pedido y registra el evento {@code OrderCancelled}.
     *
     * @param id identificador
     * @return el pedido cancelado
     * @throws OrderNotFoundException si no existe
     * @throws OrderAlreadyCancelledException si ya estaba cancelado
     */
    @Transactional
    public Order cancel(UUID id) {
        Order order = orders.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
        if (order.getStatus() == OrderStatus.CANCELLED) {
            throw new OrderAlreadyCancelledException(id);
        }
        order.cancel();
        outbox.record("Order", id.toString(), "OrderCancelled", payload(order));
        return order;
    }

    /**
     * Busca un pedido.
     *
     * @param id identificador
     * @return el pedido
     * @throws OrderNotFoundException si no existe
     */
    @Transactional(readOnly = true)
    public Order get(UUID id) {
        return orders.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
    }

    private static OrderEventPayload payload(Order order) {
        return new OrderEventPayload(order.getId(), order.getCustomer(), order.getAmount(), order.getStatus().name());
    }

    /**
     * Contenido de los eventos de pedido.
     *
     * @param orderId pedido
     * @param customer cliente
     * @param amount importe
     * @param status estado tras el cambio
     */
    public record OrderEventPayload(UUID orderId, String customer, BigDecimal amount, String status) {
    }
}
