package com.kafka.kafkastart.controller;

import com.kafka.kafkastart.model.LibraryEvent;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/libevent")
public class LibraryEventController {

  @PostMapping
  public ResponseEntity<LibraryEvent> postEvent(@RequestBody LibraryEvent libraryEvent) {
    // kafka

    return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
  }
}
