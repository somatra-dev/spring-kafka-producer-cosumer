package co.istad.producer.service;

import co.istad.producer.dto.OrderEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class OrderProducerService {

    private static final Logger log = LoggerFactory.getLogger(OrderProducerService.class);
    private static final String TOPIC = "orders-topic";

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderProducerService(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrder(OrderEvent order) {
        enrichOrder(order);

        String key = order.getCustomerId() != null ? order.getCustomerId() : order.getOrderId();

        var record = new ProducerRecord<String, OrderEvent>(
                TOPIC,
                null, // partition
                System.currentTimeMillis(),
                key,
                order,
                List.of(
                        new RecordHeader("event-type", "order.created".getBytes(StandardCharsets.UTF_8)),
                        new RecordHeader("source", "order-producer".getBytes(StandardCharsets.UTF_8)),
                        new RecordHeader("id", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8))
                )
        );

        log.info("Sending order {} | Key: {} | Customer: {}", order.getOrderId(), key, order.getCustomerId());

        kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        var metadata = result.getRecordMetadata();
                        log.info("Sent! Topic={}, Partition={}, Offset={}, Key={}",
                                metadata.topic(), metadata.partition(), metadata.offset(), key);
                    } else {
                        log.error("Failed to send order {}", order.getOrderId(), ex);
                    }
                });
    }

    public void sendToPartition(OrderEvent order, int partition) {
        enrichOrder(order);
        String key = order.getCustomerId() != null ? order.getCustomerId() : order.getOrderId();

        var record = new ProducerRecord<String, OrderEvent>(TOPIC, partition, key, order);
        kafkaTemplate.send(record);
        log.info("Sent to partition {} → Order {}", partition, order.getOrderId());
    }

    private void enrichOrder(OrderEvent order) {
        if (order.getOrderId() == null || order.getOrderId().isBlank()) {
            order.setOrderId("ORD-" + UUID.randomUUID().toString().substring(0, 8));
        }
        if (order.getTimestamp() == null) {
            order.setTimestamp(LocalDateTime.now());
        }
    }
}