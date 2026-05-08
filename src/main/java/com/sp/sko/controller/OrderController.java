package com.sp.sko.controller;

import com.sp.sko.kafka.OrderSender;
import com.sp.sko.model.OrderEvent;
import com.sp.sko.model.OrderRequest;
import com.sp.sko.model.TokenBucket;
import com.sp.sko.service.OrderProcessService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {
    private static final int DEFAULT_BUCKET_CAPACITY = 5;
    private static final long DEFAULT_TOKEN_REFILL_PERIOD_MILLIS = 5000;
    private static final long DEFAULT_NUM_TOKENS_REFILL = 2;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OrderProcessService orderProcessService;
    private final Map<String, TokenBucket> buckets = new ConcurrentHashMap<>();

    @PostMapping
    public ResponseEntity<@NonNull String> createOrder(@RequestBody OrderRequest request) throws ExecutionException, InterruptedException {
        log.info("Received order request for user: {}", request.userId());
        TokenBucket bucket = buckets.computeIfAbsent(request.userId(), id -> new TokenBucket(DEFAULT_BUCKET_CAPACITY, DEFAULT_NUM_TOKENS_REFILL, DEFAULT_TOKEN_REFILL_PERIOD_MILLIS));

        if(!bucket.tryConsume(1)){
            log.warn("No tokens available");
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body("Insufficient tokens, try later\n");
        }
        String orderId = UUID.randomUUID().toString();
        orderProcessService.process(new OrderEvent(orderId, request.userId(), request.amount()));
        return ResponseEntity.ok("Order created with order-id: %s".formatted(orderId));
    }
}
