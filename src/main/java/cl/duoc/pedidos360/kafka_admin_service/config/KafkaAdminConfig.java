package cl.duoc.pedidos360.kafka_admin_service.config;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class KafkaAdminConfig {

    @Value("${app.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /** Cliente de administracion de Kafka. Si el cluster esta caido, las llamadas fallan con timeout (503). */
    @Bean(destroyMethod = "close")
    public Admin adminClient() {
        return Admin.create(Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 8000));
    }
}
