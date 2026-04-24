package com.sp.sko.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OrderListener {

    @KafkaListener(topics = "order-placed", errorHandler = "customErrorHandler")
    public void listenOrderPlaced(String message){
        log.info("Received OrderPlaced event: {}", message);

        if(Math.random() < 0.3){
            log.error("Simulated failure - throwing exception");
            throw new RuntimeException("Payment processing failed");
        }

        log.info("Successfully processed order");
    }
}
