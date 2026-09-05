package com.rabbitmq.order_worker.repository;

import com.rabbitmq.order_worker.entity.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface OrderRepository extends JpaRepository<Order, String> {

    @Modifying
    @Transactional
    @Query(value = """
            INSERT INTO orders (order_id, product, quantity, status, created_at, updated_at)
            VALUES (:orderId, :product, :quantity, :status, now(), now())
            ON CONFLICT (order_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfNotExists(
            @Param("orderId") String orderId,
            @Param("product") String product,
            @Param("quantity") int quantity,
            @Param("status") String status
    );
}