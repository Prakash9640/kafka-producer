package com.microservices.kafkaproducer.service;

import com.microservices.kafkaproducer.dto.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessageProducer {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;


    /**
     * This method sends a message to the Kafka topic named "consumer-topic".
     * It uses the KafkaTemplate to send the message asynchronously and handles the result or exception.
     * @param message The message to be sent to the Kafka topic.
     */
    public void sendMessageToTopic(String message) {
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("consumer-topic", message);
        future.whenComplete((result, exception) -> {
            if(exception == null){
                System.out.println("sent message =[ " + message +" ] with the offset=[ " + result.getRecordMetadata().offset() + " ]");
            }else {
                System.out.println("Unable to send message=[ " + message + " ] due to : " + exception.getMessage());
            }
        });
    }
    /**
     * This method sends a Customer object to the Kafka topic named "customer-topic".
     * It uses the KafkaTemplate to send the message asynchronously and handles the result or exception.
     * @param customer The Customer object to be sent to the Kafka topic.
     */
    public void sendCustomerMessageToTopic(Customer customer) {
        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("customer-topic", customer);
            future.whenComplete((result, exception) -> {
                if (exception == null) {
                    System.out.println("sent message =[ " + customer.toString() + " ] with the offset=[ " + result.getRecordMetadata().offset() + " ]");
                } else {
                    System.out.println("Unable to send message=[ " + customer.toString() + " ] due to : " + exception.getMessage());
                }
            });
        } catch (Exception e) {
            System.out.println("Exception occurred while sending customer message: " + e.getMessage());
        }
    }
}
