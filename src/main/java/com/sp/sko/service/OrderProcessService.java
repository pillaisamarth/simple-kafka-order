package com.sp.sko.service;

import com.sp.sko.kafka.OrderSender;
import com.sp.sko.model.OrderEvent;
import com.sp.sko.model.OrderRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderProcessService {
    private final JsonMapper jsonMapper;
    private final OrderSender orderSender;

    public void process(OrderEvent orderEvent){
        String orderId = orderEvent.orderId();
        log.info("Created order with id: {}", orderId);
        String serializedOrderEvent = jsonMapper.writeValueAsString(orderEvent);
        orderSender.send(serializedOrderEvent);
    }
}
