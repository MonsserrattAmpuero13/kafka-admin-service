package cl.duoc.pedidos360.kafka_admin_service.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

/** Kafka solo permite AUMENTAR el numero de particiones, nunca reducirlo. */
public record PartitionsRequest(
        @Min(value = 1, message = "Debe tener al menos 1 particion")
        @Max(value = 50, message = "No puede superar 50 particiones")
        int totalPartitions) {}
