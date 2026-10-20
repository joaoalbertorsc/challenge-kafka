package br.com.challenge.kafka.dto;

import jakarta.validation.constraints.NotBlank;

public record MessageRequest(@NotBlank(message = "Message must not be blank") String message) {
}
