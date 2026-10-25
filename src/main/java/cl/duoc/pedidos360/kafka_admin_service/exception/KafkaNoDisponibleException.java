package cl.duoc.pedidos360.kafka_admin_service.exception;

public class KafkaNoDisponibleException extends RuntimeException {
    public KafkaNoDisponibleException() {
        super("No se pudo contactar al cluster de Kafka (tiempo de espera agotado)");
    }
}
