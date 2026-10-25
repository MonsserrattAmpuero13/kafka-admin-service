package cl.duoc.pedidos360.kafka_admin_service.dto;

import java.util.List;
import java.util.Map;

public record TopicDetalle(String name, int partitions, int replicationFactor,
                           Map<String, String> configuracion, List<ParticionInfo> particiones) {}
