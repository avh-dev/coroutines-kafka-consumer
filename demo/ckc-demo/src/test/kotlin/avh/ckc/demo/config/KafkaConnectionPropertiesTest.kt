package avh.ckc.demo.config

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class KafkaConnectionPropertiesTest {
    @Test
    fun `warmup connection contains only its independent connection settings`() {
        val properties = DemoApplicationProperties.KafkaConnection(
            bootstrapServers = "warmup-kafka:9093",
            securityProtocol = "SSL",
            sslTruststoreCertificates = "certificate",
            sslEndpointIdentificationAlgorithm = "https"
        ).kafkaConnectionProperties()

        assertEquals("warmup-kafka:9093", properties[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG])
        assertEquals("SSL", properties[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG])
        assertEquals("certificate", properties[SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG])
        assertEquals("PEM", properties[SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG])
        assertEquals("https", properties[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG])
        assertFalse(properties.containsKey("group.id"))
    }

    @Test
    fun `explicit empty endpoint identification algorithm is preserved`() {
        val properties = DemoApplicationProperties.KafkaConnection(
            sslEndpointIdentificationAlgorithm = ""
        ).kafkaConnectionProperties()

        assertEquals("", properties[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG])
    }
}
