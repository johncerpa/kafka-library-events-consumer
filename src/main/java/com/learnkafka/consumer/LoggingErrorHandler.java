package com.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ErrorHandler;

public class LoggingErrorHandler implements ErrorHandler {
    private static final Logger logger = LoggerFactory.getLogger(LoggingErrorHandler.class);

    @Override
    public void handle(Exception thrownException, ConsumerRecord<?, ?> data) {
        logger.error("Exception -> {}", thrownException);
    }
}
