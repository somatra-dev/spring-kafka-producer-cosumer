package co.istad.kh.producer_demo.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/v1/publishers")
@RequiredArgsConstructor
public class PublisherController {

    private static final String TOPIC = "events-topic";

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @PostMapping
    public ResponseEntity<String> publish(@RequestBody OrderEvent order) {
        kafkaTemplate.send(TOPIC, order.getOrderId(), order);
        return ResponseEntity.ok("Published: " + order.getOrderId());
    }


}
