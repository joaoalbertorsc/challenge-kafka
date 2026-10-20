package br.com.challenge.kafka;

import br.com.challenge.kafka.consumer.MessageConsumer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = "messages-topic")
class MessageFlowIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private MessageConsumer consumer;

    @Test
    void messagesSentThroughEndpointShouldBeConsumedInOrderAndUnchanged() throws Exception {
        List<String> messages = List.of("Hello, Kafka!", "Olá, café ☕", "Third message");

        for (String message : messages) {
            mockMvc.perform(post("/api/messages")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"message\": \"" + message + "\"}"))
                    .andExpect(status().isAccepted())
                    .andExpect(jsonPath("$.status").value("PUBLISHED"))
                    .andExpect(jsonPath("$.topic").value("messages-topic"));
        }

        long deadline = System.currentTimeMillis() + 10_000;
        while (consumer.getReceivedMessages().size() < messages.size() && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
        assertThat(consumer.getReceivedMessages()).containsExactlyElementsOf(messages);
    }
}
