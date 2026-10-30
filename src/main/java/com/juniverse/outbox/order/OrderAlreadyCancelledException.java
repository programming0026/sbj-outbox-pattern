package com.juniverse.outbox.order;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/** El pedido ya estaba cancelado (409). */
@ResponseStatus(HttpStatus.CONFLICT)
public class OrderAlreadyCancelledException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Crea la excepción.
     *
     * @param id pedido
     */
    public OrderAlreadyCancelledException(java.util.UUID id) {
        super("El pedido " + id + " ya está cancelado");
    }
}
