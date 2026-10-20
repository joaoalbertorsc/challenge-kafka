package br.com.challenge.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Creates the topic on startup (via KafkaAdmin) if it does not exist yet.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic challengeTopic(@Value("${app.kafka.topic}") String topic) {
        return TopicBuilder.name(topic)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
