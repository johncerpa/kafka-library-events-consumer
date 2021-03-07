import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.consumer.LibraryEventsConsumer;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SuppressWarnings("ALL")
@SpringBootTest
@EmbeddedKafka(topics = {"libraryEvents"}, partitions = 3)
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
public class LibraryEventsConsumerIntegTest {
    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        // Given
        Book book = Book.builder().bookId(1).bookAuthor("John").bookName("Discovering Kafka").build();

        LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(2).book(book).build();

        String json = objectMapper.writeValueAsString(libraryEvent);

        // When
        kafkaTemplate.sendDefault(json).get();

        // Block the thread for 3 seconds because Kafka works asynchronously
        CountDownLatch latch = new CountDownLatch(1); // Counts down to 0 and releases the thread
        latch.await(3, TimeUnit.SECONDS);

        // Then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assert libraryEventList.size() == 1;
        libraryEventList.forEach(le -> {
            assert le.getLibraryEventId() != null;
            assertEquals(1, le.getBook().getBookId());
        });
    }

    @Test
    void publishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        // Given
        Book book = Book.builder().bookId(1).bookAuthor("John").bookName("Discovering Kafka").build();
        LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(2).book(book).build();

        libraryEventsRepository.save(libraryEvent);

        Book updatedBook = Book.builder().bookId(1).bookName("Spring Boot with Kafka").bookAuthor("John").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);

        // When
        kafkaTemplate.sendDefault(updatedJson).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // Then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();
        assertEquals("Spring Boot with Kafka", persistedLibraryEvent.getBook().getBookName());
    }

    @Test
    void updateLibraryEvent_failure() throws JsonProcessingException, ExecutionException, InterruptedException {
        // Given
        Book book = Book.builder().bookId(1).bookAuthor("John").bookName("Discovering Kafka").build();
        LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(5).book(book).build();
        // LibraryEventType not set, so it should not updated this
        libraryEventsRepository.save(libraryEvent);

        Book updatedBook = Book.builder().bookId(1).bookName("Spring Boot with Kafka").bookAuthor("John").build();
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);

        // When
        kafkaTemplate.sendDefault(updatedJson).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // Then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();
        assertEquals("Discovering Kafka", persistedLibraryEvent.getBook().getBookName());
    }
}
