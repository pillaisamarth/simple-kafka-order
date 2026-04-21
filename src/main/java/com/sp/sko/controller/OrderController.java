package com.sp.sko.controller;

import com.sp.sko.model.OrderRequest;
import com.sp.sko.model.TokenBucket;
import org.jspecify.annotations.NonNull;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@RestController
@RequestMapping("/orders")
public class OrderController {
    private static final int DEFAULT_BUCKET_CAPACITY = 5;
    private static final long DEFAULT_TOKEN_REFILL_PERIOD_MILLIS = 5000;
    private static final long DEFAULT_NUM_TOKENS_REFILL = 2;
    private final Map<String, TokenBucket> buckets = new ConcurrentHashMap<>();

    @PostMapping
    public ResponseEntity<@NonNull String> createOrder(@RequestBody OrderRequest request){
        System.out.println("Received order for user: " + request.userId());
        TokenBucket bucket = buckets.computeIfAbsent(request.userId(), id -> new TokenBucket(DEFAULT_BUCKET_CAPACITY, DEFAULT_NUM_TOKENS_REFILL, DEFAULT_TOKEN_REFILL_PERIOD_MILLIS));

        if(!bucket.tryConsume(1)){
            System.out.println("[" + Thread.currentThread().getName() + "] - No tokens available");
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body("Insufficient tokens, try later\n");
        }
        return ResponseEntity.ok("Order received with id: " + UUID.randomUUID() + "\n");
    }
}
