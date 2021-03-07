package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {
    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("Library event saved -> {}", libraryEvent);

        switch (libraryEvent.getLibraryEventType()) {
            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
                log.info("Invalid LibraryEvent type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("LibraryEvent Id is missing");
        }

        Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());

        if (libraryEventOptional.isEmpty()) {
            throw new IllegalArgumentException("Not a valid LibraryEvent");
        }

        log.info("LibraryEvent is valid : {}", libraryEventOptional.get());
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("Successfully saved LibraryEvent");
    }

    public void handleRecovery(ConsumerRecord<Integer, String> consumerRecord) {
        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(consumerRecord.key(), consumerRecord.value());
        listenableFuture.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("Error sending the message, exception is {}", ex.getMessage());
                try {
                    throw ex;
                } catch (Throwable throwable) {
                    log.error("Error on onFailure {}", throwable.getMessage());
                }
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                log.info("key: {}, value: {}, result: {}", consumerRecord.key(), consumerRecord.value(), result);
            }
        });
    }
}
