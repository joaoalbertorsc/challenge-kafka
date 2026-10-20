package br.com.challenge.kafka.controller;

import br.com.challenge.kafka.consumer.MessageConsumer;
import br.com.challenge.kafka.dto.MessageRequest;
import br.com.challenge.kafka.dto.MessageResponse;
import br.com.challenge.kafka.producer.MessageProducer;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/messages")
public class MessageController {

    private final MessageProducer producer;
    private final MessageConsumer consumer;

    public MessageController(MessageProducer producer, MessageConsumer consumer) {
        this.producer = producer;
        this.consumer = consumer;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public MessageResponse send(@Valid @RequestBody MessageRequest request) {
        var metadata = producer.send(request.message()).join().getRecordMetadata();
        return new MessageResponse("PUBLISHED", metadata.topic(), metadata.partition(),
                metadata.offset(), request.message());
    }

    @GetMapping
    public List<String> received() {
        return consumer.getReceivedMessages();
    }
}
