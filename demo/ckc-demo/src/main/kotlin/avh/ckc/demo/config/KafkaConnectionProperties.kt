package avh.ckc.demo.config

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs

/** Builds only connection settings, so warm-up clients never inherit the main Kafka connection. */
internal fun DemoApplicationProperties.KafkaConnection.kafkaConnectionProperties(): Map<String, Any?> =
    buildMap {
        put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol)
        sslTruststoreCertificates.takeIf(String::isNotBlank)?.let {
            put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, it)
            put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM")
        }
        // An explicit empty value disables hostname verification and must not be discarded.
        sslEndpointIdentificationAlgorithm?.let {
            put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, it)
        }
    }
