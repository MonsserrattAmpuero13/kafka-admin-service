package cl.duoc.pedidos360.kafka_admin_service.dto;

import java.util.List;

/** Estado de una particion: broker lider, brokers con copia y brokers sincronizados (ISR). */
public record ParticionInfo(int id, int lider, List<Integer> replicas, List<Integer> sincronizadas) {}
