package cl.duoc.pedidos360.kafka_admin_service.dto;

import java.util.List;

public record LagGrupo(String groupId, long lagTotal, List<LagParticion> detalle) {}
