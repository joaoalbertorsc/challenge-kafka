package br.com.challenge.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessageConsumerTest {

    @Test
    void shouldStoreConsumedMessagesAndExposeThemAsReadOnly() {
        var consumer = new MessageConsumer();

        consumer.consume(new ConsumerRecord<>("messages-topic", 0, 0L, null, "Hello"));

        assertThat(consumer.getReceivedMessages()).containsExactly("Hello");
        assertThatThrownBy(() -> consumer.getReceivedMessages().add("Injected"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
