package cl.duoc.pedidos360.kafka_admin_service.service;

import cl.duoc.pedidos360.kafka_admin_service.dto.TopicConfigRequest;
import cl.duoc.pedidos360.kafka_admin_service.dto.TopicRequest;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.IncrementalAlterConfigsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaAdminServiceTest {

    @Mock Admin admin;
    @Mock CreateTopicsResult crearResultado;
    @Mock DeleteTopicsResult borrarResultado;
    @Mock ListTopicsResult listarResultado;
    @Mock IncrementalAlterConfigsResult configResultado;

    KafkaAdminService service;

    @BeforeEach
    void setUp() {
        service = new KafkaAdminService(admin);
    }

    @Test
    @SuppressWarnings("unchecked")
    void crearTopicEnviaParticionesReplicasYConfiguracion() {
        when(admin.createTopics(any(Collection.class))).thenReturn(crearResultado);
        when(crearResultado.all()).thenReturn(KafkaFuture.completedFuture(null));

        service.crearTopic(new TopicRequest("pedidos-test", 3, 3, 604800000L, "delete", 2));

        ArgumentCaptor<Collection<org.apache.kafka.clients.admin.NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(admin).createTopics(captor.capture());
        var topic = captor.getValue().iterator().next();
        assertEquals("pedidos-test", topic.name());
        assertEquals(3, topic.numPartitions());
        assertEquals(3, topic.replicationFactor());
        assertEquals("604800000", topic.configs().get("retention.ms"));
        assertEquals("delete", topic.configs().get("cleanup.policy"));
        assertEquals("2", topic.configs().get("min.insync.replicas"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void crearTopicExistenteDejaPasarElErrorOriginal() {
        KafkaFutureImpl<Void> fallo = new KafkaFutureImpl<>();
        fallo.completeExceptionally(new TopicExistsException("ya existe"));
        when(admin.createTopics(any(Collection.class))).thenReturn(crearResultado);
        when(crearResultado.all()).thenReturn(fallo);

        assertThrows(TopicExistsException.class,
                () -> service.crearTopic(new TopicRequest("pedidos-creados", 3, 3, null, null, null)));
    }

    @Test
    void nombreInvalidoSeRechazaSinLlamarAKafka() {
        assertThrows(IllegalArgumentException.class, () -> service.eliminarTopic("../malo nombre"));
        verifyNoInteractions(admin);
    }

    @Test
    void listarTopicsLosEntregaOrdenados() {
        when(admin.listTopics()).thenReturn(listarResultado);
        when(listarResultado.names()).thenReturn(KafkaFuture.completedFuture(Set.of("b-topic", "a-topic")));

        assertEquals(List.of("a-topic", "b-topic"), service.listarTopics());
    }

    @Test
    @SuppressWarnings("unchecked")
    void eliminarTopicLlamaAKafka() {
        when(admin.deleteTopics(any(Collection.class))).thenReturn(borrarResultado);
        when(borrarResultado.all()).thenReturn(KafkaFuture.completedFuture(null));

        service.eliminarTopic("pedidos-test");

        verify(admin).deleteTopics(List.of("pedidos-test"));
    }

    @Test
    void modificarConfigSinCambiosEsInvalido() {
        assertThrows(IllegalArgumentException.class,
                () -> service.modificarConfig("pedidos-creados", new TopicConfigRequest(null, null, null)));
        verify(admin, never()).incrementalAlterConfigs(any());
    }

    @Test
    void modificarConfigAplicaLosCambios() {
        when(admin.incrementalAlterConfigs(any())).thenReturn(configResultado);
        when(configResultado.all()).thenReturn(KafkaFuture.completedFuture(null));

        service.modificarConfig("pedidos-creados", new TopicConfigRequest(86400000L, "delete", null));

        verify(admin).incrementalAlterConfigs(any());
    }
}
