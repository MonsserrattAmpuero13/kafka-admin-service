package cl.duoc.pedidos360.kafka_admin_service.dto;

public record TopicResumen(String name, int partitions, int replicationFactor) {}
