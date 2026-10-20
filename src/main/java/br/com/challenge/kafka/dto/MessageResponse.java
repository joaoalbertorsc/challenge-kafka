package br.com.challenge.kafka.dto;

public record MessageResponse(String status, String topic, int partition, long offset, String message) {
}
