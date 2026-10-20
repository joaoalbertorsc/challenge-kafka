package br.com.challenge.kafka.controller;

import br.com.challenge.kafka.consumer.MessageConsumer;
import br.com.challenge.kafka.producer.MessageProducer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.KafkaException;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(MessageController.class)
class MessageControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private MessageProducer producer;

    @MockitoBean
    private MessageConsumer consumer;

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"message\": \"\"}",      // empty
            "{\"message\": \"   \"}",   // whitespace only
            "{}",                       // missing field
            "",                         // no body
            "{\"message\": "            // malformed JSON
    })
    void invalidRequestShouldReturnBadRequestWithoutPublishing(String body) throws Exception {
        mockMvc.perform(post("/api/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(producer);
    }

    @Test
    void kafkaFailureShouldReturnServiceUnavailable() throws Exception {
        when(producer.send(anyString()))
                .thenReturn(CompletableFuture.failedFuture(new KafkaException("Broker unavailable")));

        mockMvc.perform(post("/api/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"message\": \"Hello\"}"))
                .andExpect(status().isServiceUnavailable());
    }
}
