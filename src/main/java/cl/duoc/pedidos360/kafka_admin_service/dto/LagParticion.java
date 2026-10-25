package cl.duoc.pedidos360.kafka_admin_service.dto;

/** lag = ultimoOffset - offsetConfirmado: cuantos mensajes le faltan al grupo por leer en esa particion. */
public record LagParticion(String topic, int particion, long offsetConfirmado, long ultimoOffset, long lag) {}
