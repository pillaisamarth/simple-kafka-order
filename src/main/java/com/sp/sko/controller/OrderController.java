package com.sp.sko.controller;

import com.sp.sko.model.OrderRequest;
import com.sp.sko.model.TokenBucket;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {
    private static final int DEFAULT_BUCKET_CAPACITY = 5;
    private static final long DEFAULT_TOKEN_REFILL_PERIOD_MILLIS = 5000;
    private static final long DEFAULT_NUM_TOKENS_REFILL = 2;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Map<String, TokenBucket> buckets = new ConcurrentHashMap<>();

    @PostMapping
    public ResponseEntity<@NonNull String> createOrder(@RequestBody OrderRequest request){
        log.info("Received order request for user: {}", request.userId());
        TokenBucket bucket = buckets.computeIfAbsent(request.userId(), id -> new TokenBucket(DEFAULT_BUCKET_CAPACITY, DEFAULT_NUM_TOKENS_REFILL, DEFAULT_TOKEN_REFILL_PERIOD_MILLIS));

        if(!bucket.tryConsume(1)){
            log.warn("No tokens available");
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body("Insufficient tokens, try later\n");
        }
        String uuid = UUID.randomUUID().toString();
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("order-placed", uuid, "OrderPlaced for %s".formatted(request.userId()));
        future.whenComplete((sr, ex) -> {
            if(ex != null){
                log.error("Error occured while publishing event to order topic", ex);
            }else{
                log.info("OrderPlaced event sent to order topic with uuid: {}, offset:{}, partition:{}", uuid, sr.getRecordMetadata().offset(), sr.getRecordMetadata().partition());
            }
        });
        log.info("Sending response for id: {}", uuid);
        return ResponseEntity.ok("Order received with id: " + uuid + "\n");
    }
}
