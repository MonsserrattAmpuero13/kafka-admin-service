package cl.duoc.pedidos360.kafka_admin_service.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;

/** Cambios de configuracion de un topico existente. Se envian solo los campos que se quieren cambiar. */
public record TopicConfigRequest(
        @Min(value = 60000, message = "La retencion minima es 60000 ms (1 minuto)")
        Long retentionMs,
        @Pattern(regexp = "delete|compact|compact,delete",
                 message = "La politica de limpieza debe ser delete, compact o compact,delete")
        String cleanupPolicy,
        @Min(value = 1, message = "min.insync.replicas debe ser al menos 1")
        @Max(value = 3, message = "min.insync.replicas no puede superar 3")
        Integer minInsyncReplicas) {}
