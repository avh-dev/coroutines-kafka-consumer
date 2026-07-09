package avh.ckc.spring

import avh.ckc.core.ProcessingMode
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties(prefix = "ckc")
data class CkcConsumerProperties(
    var enabled: Boolean = true,
    var metrics: Metrics = Metrics(),
    var defaultCluster: String? = null,
    var clusters: MutableMap<String, Cluster> = linkedMapOf(),
    var consumers: MutableMap<String, Consumer> = linkedMapOf()
) {
    data class Metrics(
        var enabled: Boolean = true,
        var prefix: String = "app"
    )

    data class Cluster(
        var kafkaProperties: MutableMap<String, String> = linkedMapOf()
    )

    data class Consumer(
        var autoStartup: Boolean = true,
        var cluster: String? = null,
        var topics: List<String> = emptyList(),
        var topicPattern: String? = null,
        var groupId: String? = null,
        var clientId: String? = null,
        var keyDeserializer: String? = null,
        var valueDeserializer: String? = null,
        var processingMode: ProcessingMode = ProcessingMode.AT_LEAST_ONCE_UNORDERED,
        var workerConcurrency: Int = 1,
        var consumerPollLoopConcurrency: Int = 1,
        var commitInterval: Duration = Duration.ofSeconds(5),
        var workChannelCapacity: Int = 1024,
        var retry: Retry = Retry(),
        var kafkaProperties: MutableMap<String, String> = linkedMapOf()
    ) {
        internal fun kafkaProperties(clusterProperties: Map<String, String>): Map<String, Any?> {
            val merged = linkedMapOf<String, Any?>()
            merged.putAll(clusterProperties)
            merged.putAll(kafkaProperties)
            groupId?.let { merged[ConsumerConfig.GROUP_ID_CONFIG] = it }
            clientId?.let { merged[ConsumerConfig.CLIENT_ID_CONFIG] = it }
            keyDeserializer?.let { merged[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = it }
            valueDeserializer?.let { merged[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = it }
            return merged
        }
    }

    data class Retry(
        var maxRetries: Int = 0,
        var delay: Duration = Duration.ZERO
    )
}
