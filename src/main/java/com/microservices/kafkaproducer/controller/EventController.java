package com.microservices.kafkaproducer.controller;

import com.microservices.kafkaproducer.dto.Customer;
import com.microservices.kafkaproducer.service.KafkaMessageProducer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer-app")
public class EventController {

    private final KafkaMessageProducer kafkaMessageProducer;

    public EventController(KafkaMessageProducer kafkaMessageProducer) {
        this.kafkaMessageProducer = kafkaMessageProducer;
    }

    @GetMapping("/publish/{message}")
    public ResponseEntity<?> publishMessage(@PathVariable String message) {
        try{
            for(int i=1; i<=10000; i++){
                String msg = message + " : " + i;
                kafkaMessageProducer.sendMessageToTopic(msg);
            }
            return ResponseEntity.ok("Message sent to Kafka topic successfully.");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                            .body("Failed to send message to Kafka topic: " + e.getMessage());
        }
    }

    @PostMapping("/publish-customer")
    public ResponseEntity<?> publishCustomerMessage(@RequestBody Customer customer){
        try{
            kafkaMessageProducer.sendCustomerMessageToTopic(customer);
            return ResponseEntity.ok("Customer message sent to Kafka topic successfully.");
        }catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).
                    body("Failed to send customer message to Kafka topic: " + e.getMessage());
        }
    }
}
