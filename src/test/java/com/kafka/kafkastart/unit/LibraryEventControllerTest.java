package com.kafka.kafkastart.unit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kafkastart.controller.LibraryEventController;
import com.kafka.kafkastart.model.Book;
import com.kafka.kafkastart.model.LibraryEvent;
import com.kafka.kafkastart.producer.LibraryEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventController.class)
@AutoConfigureMockMvc
class LibraryEventControllerTest {

  @MockBean LibraryEventProducer libraryEventProducer;
  @Autowired private MockMvc mockMvc;
  @Autowired private ObjectMapper objectMapper;

  @Test
  void postEvent() throws Exception {
    final Book book = Book.builder().bookId(123).bookAuthor("Test").bookName("test").build();
    final LibraryEvent libraryEvent =
        LibraryEvent.builder()
            .libraryEventId(null)
            .book(book)
            .build();

    final String json = objectMapper.writeValueAsString(libraryEvent);
    doNothing().when(libraryEventProducer).sendLibraryEvent(isA(LibraryEvent.class));

    mockMvc
        .perform(post("/v1/libevent").content(json).contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated());
  }
}
