package com.juniverse.outbox.controller;

import com.juniverse.outbox.dto.OrderResponse;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.juniverse.outbox.dto.CreateOrderRequest;
import com.juniverse.outbox.service.OrderService;

/** API REST de pedidos en {@code /api/orders}. */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService service;

    /**
     * Crea el controlador.
     *
     * @param service servicio de pedidos
     */
    public OrderController(OrderService service) {
        this.service = service;
    }

    /**
     * Crea un pedido (y, en la misma transacción, su evento del outbox).
     *
     * @param request datos validados
     * @return 201 con el pedido y la cabecera Location
     */
    @PostMapping
    public ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
        OrderResponse created = OrderResponse.from(service.create(request));
        return ResponseEntity.created(URI.create("/api/orders/" + created.id())).body(created);
    }

    /**
     * Consulta un pedido.
     *
     * @param id identificador
     * @return el pedido
     */
    @GetMapping("/{id}")
    public OrderResponse get(@PathVariable UUID id) {
        return OrderResponse.from(service.get(id));
    }

    /**
     * Cancela un pedido (genera el evento {@code OrderCancelled}).
     *
     * @param id identificador
     * @return el pedido cancelado
     */
    @PostMapping("/{id}/cancel")
    public OrderResponse cancel(@PathVariable UUID id) {
        return OrderResponse.from(service.cancel(id));
    }
}
