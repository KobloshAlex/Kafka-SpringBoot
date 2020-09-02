package com.kafka.kafkastart.integrational.controller;

import com.kafka.kafkastart.model.Book;
import com.kafka.kafkastart.model.LibraryEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
    topics = {"library-events"},
    partitions = 3)
@TestPropertySource(
    properties = {
      "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "spring.kafka.admin.properties.bootstrap-servers=${spring.embedded.kafka.brokers}"
    })
class LibraryEventControllerTest {

  private Consumer<Integer, String> consumer;

  @Autowired private TestRestTemplate testRestTemplate;

  @Autowired private EmbeddedKafkaBroker embeddedKafkaBroker;

  @BeforeEach
  void setUp() {
    Map<String, Object> configs =
        new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
    consumer =
        new DefaultKafkaConsumerFactory<>(
                configs, new IntegerDeserializer(), new StringDeserializer())
            .createConsumer();
    embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
  }

  @AfterEach
  void tearDown() {
    consumer.close();
  }

  @Test
  @Timeout(5)
  void postLibraryEvent() {

    final LibraryEvent libraryEvent =
        LibraryEvent.builder()
            .libraryEventId(null)
            .book(Book.builder().bookId(123).bookAuthor("Test").bookName("test").build())
            .build();

    final HttpHeaders header = new HttpHeaders();
    header.set("content-type", MediaType.APPLICATION_JSON.toString());
    final HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, header);
    final ResponseEntity<LibraryEvent> responseEntity =
        testRestTemplate.exchange("/v1/libevent", HttpMethod.POST, request, LibraryEvent.class);

    assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

    final ConsumerRecord<Integer, String> consumerRecord =
        KafkaTestUtils.getSingleRecord(consumer, "library-events");
    final String expected =
        "{\"libraryEventId\":null,\"book\":{\"bookId\":123,\"bookName\":\"test\",\"bookAuthor\":\"Test\"},\"eventType\":\"NEW\"}";
    final String value = consumerRecord.value();

    assertEquals(expected, value);
  }
}
