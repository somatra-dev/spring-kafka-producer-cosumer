package co.istad.kh.consumer_demo.listener;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class EventConsumer {

    @KafkaListener(topics = "events-topic", groupId = "consumer-demo-group")
    public void consume(OrderEvent event) {
        log.info("Received: {} {} (price: {}, customer: {})",
                event.getQuantity(), event.getProduct(), event.getPrice(), event.getCustomerId());
    }
}
