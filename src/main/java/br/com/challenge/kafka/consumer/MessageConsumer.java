package br.com.challenge.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class MessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    /** Received messages, kept in memory only to make it easy to validate the flow. */
    private final List<String> receivedMessages = new CopyOnWriteArrayList<>();

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, String> record) {
        log.info("Message consumed [topic={}, partition={}, offset={}]: {}",
                record.topic(), record.partition(), record.offset(), record.value());
        receivedMessages.add(record.value());
    }

    public List<String> getReceivedMessages() {
        return List.copyOf(receivedMessages);
    }
}
