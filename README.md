# kafka-admin-service

Microservicio de **administracion de Apache Kafka** de Pedidos360. Permite crear, listar, modificar y eliminar topicos, ajustar su politica de retencion y limpieza, y consultar los consumer groups con su lag. Complementa a `servicebus-admin-service` (Azure Service Bus se mantiene; Kafka se agrega en paralelo).

- Puerto: **8083**
- Seguridad: JWT (Auth0), mismo issuer y audience que el resto del sistema
- Cluster esperado: 3 brokers (`localhost:9092,9093,9094`), levantado con `EV1/kafka/docker-compose.kafka.yml`

## Configuracion

| Variable | Descripcion | Por defecto |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Brokers del cluster | `localhost:9092,localhost:9093,localhost:9094` |

## Ejecucion

```bash
./mvnw -q test
./mvnw spring-boot:run
```

## Endpoints (`/api/kafka`)

Todos requieren `Authorization: Bearer <token>`.

| Metodo | Ruta | Que hace |
|---|---|---|
| POST | `/topics` | Crea un topico (201) |
| GET | `/topics` | Lista topicos con particiones y replicas |
| GET | `/topics/{name}` | Detalle: configuracion, lider, replicas e ISR por particion |
| PATCH | `/topics/{name}/config` | Cambia `retentionMs`, `cleanupPolicy`, `minInsyncReplicas` (204) |
| PATCH | `/topics/{name}/partitions` | Aumenta particiones (solo se puede subir) (204) |
| DELETE | `/topics/{name}` | Elimina el topico (204) |
| GET | `/consumer-groups` | Lista grupos de consumidores |
| GET | `/consumer-groups/{id}` | Estado, miembros y particiones asignadas |
| GET | `/consumer-groups/{id}/lag` | Offset confirmado, ultimo offset y lag por particion |
| DELETE | `/consumer-groups/{id}` | Elimina el grupo (solo si no tiene consumidores) |

Errores: 400 datos invalidos, 404 no existe, 409 ya existe / grupo en uso, 503 Kafka no responde.

## Ejemplos

```bash
TOKEN="<token de Auth0>"

# Crear topico con 3 particiones, 3 replicas y retencion de 7 dias
curl -X POST http://localhost:8083/api/kafka/topics \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"demo-topic","partitions":3,"replicationFactor":3,"retentionMs":604800000,"cleanupPolicy":"delete","minInsyncReplicas":2}'

# Cambiar retencion a 1 dia
curl -X PATCH http://localhost:8083/api/kafka/topics/demo-topic/config \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"retentionMs":86400000}'

# Ver el lag del grupo de notificaciones
curl http://localhost:8083/api/kafka/consumer-groups/notificaciones-group/lag \
  -H "Authorization: Bearer $TOKEN"

# Eliminar el topico de prueba
curl -X DELETE http://localhost:8083/api/kafka/topics/demo-topic -H "Authorization: Bearer $TOKEN"
```

## Politica de retencion y limpieza

- `cleanup.policy=delete`: Kafka borra los segmentos antiguos al superar `retention.ms`.
- `cleanup.policy=compact`: conserva el ultimo mensaje por clave.
- `min.insync.replicas=2` junto con `acks=all` en el productor asegura que un mensaje se confirme solo si al menos 2 de las 3 replicas lo tienen.
