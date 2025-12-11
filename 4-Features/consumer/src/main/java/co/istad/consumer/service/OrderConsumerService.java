package co.istad.consumer.service;

import co.istad.consumer.dto.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumerService {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumerService.class);
    private final String instanceId = "Consumer-" + System.nanoTime() % 10000;

    @KafkaListener(
            topics = "orders-topic",
            groupId = "order-processing-group",
            containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void consumeOrder(
            @Payload OrderEvent order,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        // THIS IS YOUR DEMO LOG — everyone will see it clearly!
        log.info("RECEIVED → [Instance: {}] OrderId={}, Product={}, Qty={}, Customer={}, [P={}, O={}, Key={}]",
                instanceId,
                order.getOrderId(),
                order.getProduct(),
                order.getQuantity(),
                order.getCustomerId(),
                partition, offset, key);

//        if (order.isPoisonPill()) {
//            log.error("POISON PILL DETECTED → {}", order.getOrderId());
//            throw new IllegalArgumentException("Quantity cannot be 0");
//        }

        // Simulate processing
//        try { Thread.sleep(300); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        log.info("PROCESSED SUCCESSFULLY → OrderId={}", order.getOrderId());
        ack.acknowledge();
    }

//    @KafkaListener(topics = "orders-topic.DLT", groupId = "dlt-group")
//    public void consumeDLT(@Payload OrderEvent order,
//                           @Header(KafkaHeaders.RECEIVED_KEY) String key,
//                           @Header(KafkaHeaders.OFFSET) long offset) {
//        log.warn("DLT RECEIVED → Failed Order: {} | Key: {} | Offset: {}",
//                order.getOrderId(), key, offset);
//    }
}