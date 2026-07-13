package avh.ckc.spring

import avh.ckc.core.ProcessingMode
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties(prefix = "ckc")
data class CkcConsumerProperties(
    var enabled: Boolean = true,
    var lifecycle: Lifecycle = Lifecycle(),
    var health: Health = Health(),
    var defaultProcessingDispatcher: String? = null,
    var dispatchers: MutableMap<String, Dispatcher> = linkedMapOf(),
    var metrics: Metrics = Metrics(),
    var defaultRetrySchema: String? = null,
    var retrySchemas: MutableMap<String, RetrySchema> = linkedMapOf(),
    var defaultCluster: String? = null,
    var clusters: MutableMap<String, Cluster> = linkedMapOf(),
    var consumers: MutableMap<String, Consumer> = linkedMapOf()
) {
    data class Lifecycle(
        var phase: Int = 0,
        var shutdownTimeout: Duration = Duration.ofSeconds(30)
    )

    data class Health(
        var enabled: Boolean = true
    )

    data class Dispatcher(
        var type: DispatcherType = DispatcherType.FIXED_THREAD_POOL,
        var threads: Int = 1,
        var threadNamePrefix: String? = null,
        var beanName: String? = null
    )

    enum class DispatcherType {
        DISPATCHERS_DEFAULT,
        DISPATCHERS_IO,
        FIXED_THREAD_POOL,
        VIRTUAL_THREAD_PER_TASK,
        BEAN
    }

    data class Metrics(
        var enabled: Boolean = true,
        var implementation: MetricsImplementation = MetricsImplementation.MICROMETER,
        var prefix: String = "app",
        var micrometer: Micrometer = Micrometer()
    )

    enum class MetricsImplementation {
        MICROMETER,
        CUSTOM,
        NONE
    }

    data class Micrometer(
        var defaultSchema: String? = null,
        var schemas: MutableMap<String, MicrometerSchema> = linkedMapOf()
    )

    data class MicrometerSchema(
        var metricPrefix: String = "app",
        var staticTags: List<MetricTag> = emptyList(),
        var recordDrivenTags: List<RecordDrivenTag> = emptyList()
    )

    data class MetricTag(
        var name: String = "",
        var value: String = ""
    )

    data class RecordDrivenTag(
        var name: String = "",
        var default: String = "NONE"
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
        var processingMode: ProcessingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
        var workerConcurrency: Int = 1,
        var consumerPollLoopConcurrency: Int = 1,
        var commitInterval: Duration = Duration.ofSeconds(5),
        var workChannelCapacity: Int = 1024,
        var processingDispatcher: String? = null,
        var metrics: ConsumerMetricsProperties = ConsumerMetricsProperties(),
        var retrySchema: String? = null,
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

    data class ConsumerMetricsProperties(
        var schema: String? = null
    )

    data class RetrySchema(
        var rules: List<RetryRule> = emptyList()
    )

    data class RetryRule(
        var exceptions: List<String> = emptyList(),
        var maxRetries: Int = 0,
        var delay: Duration = Duration.ZERO
    )
}
