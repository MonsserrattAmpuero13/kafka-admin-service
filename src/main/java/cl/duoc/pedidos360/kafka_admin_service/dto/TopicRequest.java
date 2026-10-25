package cl.duoc.pedidos360.kafka_admin_service.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/** Datos para crear un topico. retentionMs, cleanupPolicy y minInsyncReplicas son opcionales. */
public record TopicRequest(
        @NotBlank(message = "El nombre no puede estar vacio")
        @Pattern(regexp = "^[a-zA-Z0-9._-]{1,100}$",
                 message = "El nombre solo admite letras, numeros, punto, guion y guion bajo (maximo 100)")
        String name,
        @Min(value = 1, message = "Debe tener al menos 1 particion")
        @Max(value = 50, message = "No puede superar 50 particiones")
        int partitions,
        @Min(value = 1, message = "El factor de replicacion debe ser al menos 1")
        @Max(value = 3, message = "El factor de replicacion no puede superar 3 (hay 3 brokers)")
        int replicationFactor,
        @Min(value = 60000, message = "La retencion minima es 60000 ms (1 minuto)")
        Long retentionMs,
        @Pattern(regexp = "delete|compact|compact,delete",
                 message = "La politica de limpieza debe ser delete, compact o compact,delete")
        String cleanupPolicy,
        @Min(value = 1, message = "min.insync.replicas debe ser al menos 1")
        @Max(value = 3, message = "min.insync.replicas no puede superar 3")
        Integer minInsyncReplicas) {}
