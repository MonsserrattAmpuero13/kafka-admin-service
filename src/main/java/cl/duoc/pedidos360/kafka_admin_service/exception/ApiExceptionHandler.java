package cl.duoc.pedidos360.kafka_admin_service.exception;

import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.List;
import java.util.Map;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> validacion(MethodArgumentNotValidException e) {
        List<String> errores = e.getBindingResult().getFieldErrors().stream()
                .map(f -> f.getField() + ": " + f.getDefaultMessage()).toList();
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("status", 400, "error", "Datos invalidos", "detalles", errores));
    }

    @ExceptionHandler({IllegalArgumentException.class, InvalidTopicException.class,
            InvalidReplicationFactorException.class, InvalidPartitionsException.class,
            InvalidConfigurationException.class})
    public ResponseEntity<Map<String, Object>> argumentoInvalido(RuntimeException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("status", 400, "error", String.valueOf(e.getMessage())));
    }

    @ExceptionHandler({TopicExistsException.class, GroupNotEmptyException.class})
    public ResponseEntity<Map<String, Object>> conflicto(RuntimeException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("status", 409, "error", e instanceof TopicExistsException
                        ? "El topico ya existe" : "El grupo tiene consumidores conectados y no se puede eliminar"));
    }

    @ExceptionHandler({UnknownTopicOrPartitionException.class, GroupIdNotFoundException.class})
    public ResponseEntity<Map<String, Object>> noExiste(RuntimeException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("status", 404, "error", "El recurso no existe"));
    }

    @ExceptionHandler({KafkaNoDisponibleException.class, TimeoutException.class})
    public ResponseEntity<Map<String, Object>> noDisponible(RuntimeException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("status", 503, "error", "Kafka no responde. Verifica que el cluster este encendido"));
    }
}
