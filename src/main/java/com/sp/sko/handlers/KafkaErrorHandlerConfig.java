package com.sp.sko.handlers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;

@Configuration
@Slf4j
public class KafkaErrorHandlerConfig {

    @Bean
    public ConsumerAwareListenerErrorHandler customErrorHandler(){
        return (msg, ex, consumer) -> {
            log.error("Exception thrown in the listener while processing message", ex);
            return msg;
        };
    }
}
