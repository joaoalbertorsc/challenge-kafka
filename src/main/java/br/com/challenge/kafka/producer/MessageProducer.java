package br.com.challenge.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class MessageProducer {

    private static final Logger log = LoggerFactory.getLogger(MessageProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public MessageProducer(KafkaTemplate<String, String> kafkaTemplate,
                           @Value("${app.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public CompletableFuture<SendResult<String, String>> send(String message) {
        log.info("Sending message to topic '{}': {}", topic, message);

        return kafkaTemplate.send(topic, message)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send message to topic '{}'", topic, ex);
                        return;
                    }
                    var metadata = result.getRecordMetadata();
                    log.info("Message published successfully [topic={}, partition={}, offset={}]",
                            metadata.topic(), metadata.partition(), metadata.offset());
                });
    }
}
