// src/main/java/co/istad/producer/controller/OrderController.java
package co.istad.producer.controller;

import co.istad.producer.dto.OrderEvent;
import co.istad.producer.service.OrderProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final OrderProducerService producerService;
    private final Random random = new Random();
    private final AtomicInteger counter = new AtomicInteger();

    public OrderController(OrderProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping
    public ResponseEntity<String> create(@RequestBody OrderEvent order) {
        producerService.sendOrder(order);
        return ResponseEntity.ok("Order sent: " + order.getOrderId());
    }

    @PostMapping("/random")
    public String random() {
        String[] products = {"Laptop", "Phone", "Tablet", "Watch"};
        String[] customers = {"CUST-001", "CUST-002", "CUST-003", "CUST-004"};

        OrderEvent order = OrderEvent.builder()
                .orderId("ORD-" + counter.incrementAndGet())
                .product(products[random.nextInt(products.length)])
                .quantity(random.nextInt(10) + 1)
                .price(99.99 + random.nextDouble(900))
                .customerId(customers[random.nextInt(customers.length)])
                .timestamp(LocalDateTime.now())
                .build();

        producerService.sendOrder(order);
        return "Random order sent: " + order.getOrderId() + " | " + order.getProduct();
    }

    @PostMapping("/poison")
    public String poison() {
        OrderEvent poison = OrderEvent.builder()
                .orderId("POISON-" + System.currentTimeMillis())
                .product("INVALID ITEM")
                .quantity(0)
                .price(0.0)
                .customerId("CUST-666")
                .timestamp(LocalDateTime.now())
                .build();

        producerService.sendOrder(poison);
        return "POISON PILL SENT → Will trigger DLT after retries!";
    }

    @PostMapping("/batch/{count}")
    public String batch(@PathVariable int count) {
        for (int i = 0; i < count; i++) {
            random();
            try { Thread.sleep(150); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        return "Submitted " + count + " orders!";
    }

    @PostMapping("/partition/{p}")
    public String toPartition(@PathVariable int p, @RequestBody OrderEvent order) {
        producerService.sendToPartition(order, p);
        return "Sent to partition " + p;
    }

    @GetMapping("/health")
    public String health() {
        return "Producer UP | Topic: orders-topic | Port: 8081";
    }
}