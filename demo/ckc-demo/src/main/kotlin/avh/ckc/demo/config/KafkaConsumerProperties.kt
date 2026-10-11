package avh.ckc.demo.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs

internal fun DemoApplicationProperties.kafkaConsumerProperties(
    runtime: DemoApplicationProperties.ConsumerRuntime
): Map<String, Any> {
    val shared = kafka.consumer
    val topic = runtime.kafka
    return buildMap {
        put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafka.securityProtocol)
        kafka.sslTruststoreCertificates.takeIf(String::isNotBlank)?.let {
            put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, it)
            put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM")
        }
        kafka.sslEndpointIdentificationAlgorithm?.let {
            put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, it)
        }
        shared.assignmentStrategy?.trim()?.takeIf(String::isNotEmpty)?.let {
            put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, it)
        }
        put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, topic.fetchMinBytes ?: shared.fetchMinBytes)
        put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, topic.fetchMaxWaitMs ?: shared.fetchMaxWaitMs)
        put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, topic.maxPollRecords ?: shared.maxPollRecords)
        put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, shared.maxPollIntervalMs)
        put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, topic.fetchMaxBytes ?: shared.fetchMaxBytes)
        put(
            ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
            topic.maxPartitionFetchBytes ?: shared.maxPartitionFetchBytes
        )
    }
}
