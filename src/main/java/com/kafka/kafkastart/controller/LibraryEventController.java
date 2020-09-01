package com.kafka.kafkastart.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.kafkastart.model.LibraryEvent;
import com.kafka.kafkastart.producer.LibraryEventProducer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/libevent")
public class LibraryEventController {

  private final LibraryEventProducer libraryEventProducer;

  public LibraryEventController(LibraryEventProducer libraryEventProducer) {
    this.libraryEventProducer = libraryEventProducer;
  }

  @PostMapping
  public ResponseEntity<LibraryEvent> postEvent(@RequestBody LibraryEvent libraryEvent)
      throws JsonProcessingException {
    // invoke kafka producer
    libraryEventProducer.sendLibraryEvent(libraryEvent);
    return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
  }
}
