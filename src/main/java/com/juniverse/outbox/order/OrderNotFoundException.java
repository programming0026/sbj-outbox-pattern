package com.juniverse.outbox.order;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/** No existe el pedido (404). */
@ResponseStatus(HttpStatus.NOT_FOUND)
public class OrderNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Crea la excepción.
     *
     * @param id pedido buscado
     */
    public OrderNotFoundException(java.util.UUID id) {
        super("No existe el pedido " + id);
    }
}
