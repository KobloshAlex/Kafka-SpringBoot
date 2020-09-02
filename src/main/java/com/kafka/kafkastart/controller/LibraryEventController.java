package com.kafka.kafkastart.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.kafkastart.model.EventType;
import com.kafka.kafkastart.model.LibraryEvent;
import com.kafka.kafkastart.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@RestController
@RequestMapping("/v1/libevent")
@Slf4j
public class LibraryEventController {

  private final LibraryEventProducer libraryEventProducer;

  public LibraryEventController(LibraryEventProducer libraryEventProducer) {
    this.libraryEventProducer = libraryEventProducer;
  }

  @PostMapping
  public ResponseEntity<LibraryEvent> postEvent(@RequestBody @Valid LibraryEvent libraryEvent)
      throws JsonProcessingException {
    // invoke kafka producer
    libraryEvent.setEventType(EventType.NEW);
    libraryEventProducer.sendLibraryEvent(libraryEvent);

    return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
  }

  // put
}
