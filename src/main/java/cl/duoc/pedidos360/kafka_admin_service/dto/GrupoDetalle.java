package cl.duoc.pedidos360.kafka_admin_service.dto;

import java.util.List;

public record GrupoDetalle(String groupId, String estado, int miembros,
                           List<MiembroInfo> detalleMiembros, List<String> particionesAsignadas) {}
