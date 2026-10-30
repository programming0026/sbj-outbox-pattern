package com.juniverse.outbox.order;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

/** Acceso a los pedidos. */
public interface OrderRepository extends JpaRepository<Order, UUID> {
}
