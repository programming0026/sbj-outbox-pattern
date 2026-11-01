package com.juniverse.outbox.repository;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import com.juniverse.outbox.model.Order;

/** Acceso a los pedidos. */
public interface OrderRepository extends JpaRepository<Order, UUID> {
}
