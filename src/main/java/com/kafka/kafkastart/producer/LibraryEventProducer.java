package com.kafka.kafkastart.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kafkastart.model.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class LibraryEventProducer {

  final String topic = "library-events";
  private final KafkaTemplate<Integer, String> kafkaTemplate;
  private final ObjectMapper objectMapper;

  public LibraryEventProducer(
      KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
    this.kafkaTemplate = kafkaTemplate;
    this.objectMapper = objectMapper;
  }

  public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

    final String value = objectMapper.writeValueAsString(libraryEvent);
    final Integer key = libraryEvent.getLibraryEventId();
    final ListenableFuture<SendResult<Integer, String>> listenableFuture =
        kafkaTemplate.sendDefault(key, value);
    listenableFuture.addCallback(
        new ListenableFutureCallback<SendResult<Integer, String>>() {
          @Override
          public void onFailure(Throwable exception) {
            handleFailure(key, value, exception);
          }

          @Override
          public void onSuccess(SendResult<Integer, String> result) {
            handleSuccess(key, value, result);
          }
        });
  }

  public SendResult<Integer, String> sendLibraryEventSync(LibraryEvent libraryEvent)
      throws JsonProcessingException {

    final String value = objectMapper.writeValueAsString(libraryEvent);
    final Integer key = libraryEvent.getLibraryEventId();
    SendResult<Integer, String> sendResult = null;
    try {
      sendResult = kafkaTemplate.sendDefault(key, value).get();
    } catch (InterruptedException | ExecutionException exception) {
      log.error(
          "InterruptedException | ExecutionException Error sending message: {}",
          exception.getMessage());
      try {
        throw exception;
      } catch (Exception e) {
        log.error("Exception Error sending message: {}", e.getMessage());
      }
    }

    return sendResult;
  }

  public void sendLibraryEventSyncTwo(LibraryEvent libraryEvent) throws JsonProcessingException {

    final String value = objectMapper.writeValueAsString(libraryEvent);
    final Integer key = libraryEvent.getLibraryEventId();
    ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, topic);
    final ListenableFuture<SendResult<Integer, String>> listenableFuture =
        kafkaTemplate.send(producerRecord);

    listenableFuture.addCallback(
        new ListenableFutureCallback<SendResult<Integer, String>>() {
          @Override
          public void onFailure(Throwable exception) {
            handleFailure(key, value, exception);
          }

          @Override
          public void onSuccess(SendResult<Integer, String> result) {
            handleSuccess(key, value, result);
          }
        });
  }

  private ProducerRecord<Integer, String> buildProducerRecord(
      Integer key, String value, String topic) {
    return new ProducerRecord<>(topic, null, key, value, null);
  }

  private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
    log.info(
        "Message sent successfully for the key: {} and value is {}, partitions is {}",
        key,
        value,
        result.getRecordMetadata().partition());
  }

  private void handleFailure(Integer key, String value, Throwable exception) {
    log.error("Error sending message: {}", exception.getMessage());
    try {
      throw exception;
    } catch (Throwable throwable) {
      log.error("Error in OnFailure {}", throwable.getMessage());
    }
  }
}
