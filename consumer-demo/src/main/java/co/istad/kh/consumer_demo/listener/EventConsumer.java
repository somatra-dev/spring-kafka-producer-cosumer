package co.istad.kh.consumer_demo.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class EventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(EventConsumer.class);

    @KafkaListener(topics = "events-topic", groupId = "consumer-demo-group")
    public void consume(String message) {
        logger.info("Received message: {}", message);
    }
}
