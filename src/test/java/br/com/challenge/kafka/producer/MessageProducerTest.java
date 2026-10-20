package br.com.challenge.kafka.producer;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MessageProducerTest {

    @Test
    @SuppressWarnings("unchecked")
    void shouldSendMessageToConfiguredTopic() {
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send("messages-topic", "Hello"))
                .thenReturn(new CompletableFuture<SendResult<String, String>>());

        new MessageProducer(kafkaTemplate, "messages-topic").send("Hello");

        verify(kafkaTemplate).send("messages-topic", "Hello");
    }
}
