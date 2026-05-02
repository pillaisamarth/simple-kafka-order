package com.sp.sko.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderSender {
    private final KafkaTemplate<String, String> kafkaTemplate;

    public void send(String message){
        String uuid = UUID.randomUUID().toString();
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("order-placed", uuid, message);
        future.whenComplete((sr, ex) -> {
            if(ex != null){
                log.error("Error occured while publishing event to order topic", ex);
            }else{
                log.info("OrderPlaced event sent to order topic with uuid: {}, offset:{}, partition:{}", uuid, sr.getRecordMetadata().offset(), sr.getRecordMetadata().partition());
            }
        });
    }
}
