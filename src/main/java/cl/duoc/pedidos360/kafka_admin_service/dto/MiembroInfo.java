package cl.duoc.pedidos360.kafka_admin_service.dto;

import java.util.List;

/** Un consumidor del grupo y las particiones que Kafka le asigno. */
public record MiembroInfo(String clientId, String host, List<String> particiones) {}
