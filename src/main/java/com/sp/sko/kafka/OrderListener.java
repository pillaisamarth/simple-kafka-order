package com.sp.sko.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OrderListener {

    @KafkaListener(topics = "order-placed")
    public void listenOrderPlaced(String message){
        log.info("Received OrderPlaced event: {}", message);
    }
}
