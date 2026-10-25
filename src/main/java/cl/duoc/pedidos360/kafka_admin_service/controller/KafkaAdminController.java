package cl.duoc.pedidos360.kafka_admin_service.controller;

import cl.duoc.pedidos360.kafka_admin_service.dto.GrupoDetalle;
import cl.duoc.pedidos360.kafka_admin_service.dto.GrupoResumen;
import cl.duoc.pedidos360.kafka_admin_service.dto.LagGrupo;
import cl.duoc.pedidos360.kafka_admin_service.dto.PartitionsRequest;
import cl.duoc.pedidos360.kafka_admin_service.dto.TopicConfigRequest;
import cl.duoc.pedidos360.kafka_admin_service.dto.TopicDetalle;
import cl.duoc.pedidos360.kafka_admin_service.dto.TopicRequest;
import cl.duoc.pedidos360.kafka_admin_service.dto.TopicResumen;
import cl.duoc.pedidos360.kafka_admin_service.service.KafkaAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/kafka")
public class KafkaAdminController {

    private final KafkaAdminService service;

    public KafkaAdminController(KafkaAdminService service) {
        this.service = service;
    }

    // ---- Topicos ----
    @PostMapping("/topics")
    public ResponseEntity<Void> crearTopic(@Valid @RequestBody TopicRequest request) {
        service.crearTopic(request);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @GetMapping("/topics")
    public ResponseEntity<List<TopicResumen>> listarTopics() {
        return ResponseEntity.ok(service.listarTopicsConDetalle());
    }

    @GetMapping("/topics/{name}")
    public ResponseEntity<TopicDetalle> describirTopic(@PathVariable String name) {
        return ResponseEntity.ok(service.describirTopic(name));
    }

    @PatchMapping("/topics/{name}/config")
    public ResponseEntity<Void> modificarConfig(@PathVariable String name,
                                                @Valid @RequestBody TopicConfigRequest request) {
        service.modificarConfig(name, request);
        return ResponseEntity.noContent().build();
    }

    @PatchMapping("/topics/{name}/partitions")
    public ResponseEntity<Void> aumentarParticiones(@PathVariable String name,
                                                    @Valid @RequestBody PartitionsRequest request) {
        service.aumentarParticiones(name, request.totalPartitions());
        return ResponseEntity.noContent().build();
    }

    @DeleteMapping("/topics/{name}")
    public ResponseEntity<Void> eliminarTopic(@PathVariable String name) {
        service.eliminarTopic(name);
        return ResponseEntity.noContent().build();
    }

    // ---- Consumer groups ----
    @GetMapping("/consumer-groups")
    public ResponseEntity<List<GrupoResumen>> listarGrupos() {
        return ResponseEntity.ok(service.listarGrupos());
    }

    @GetMapping("/consumer-groups/{groupId}")
    public ResponseEntity<GrupoDetalle> describirGrupo(@PathVariable String groupId) {
        return ResponseEntity.ok(service.describirGrupo(groupId));
    }

    @GetMapping("/consumer-groups/{groupId}/lag")
    public ResponseEntity<LagGrupo> lagDelGrupo(@PathVariable String groupId) {
        return ResponseEntity.ok(service.lagDelGrupo(groupId));
    }

    @DeleteMapping("/consumer-groups/{groupId}")
    public ResponseEntity<Void> eliminarGrupo(@PathVariable String groupId) {
        service.eliminarGrupo(groupId);
        return ResponseEntity.noContent().build();
    }
}
