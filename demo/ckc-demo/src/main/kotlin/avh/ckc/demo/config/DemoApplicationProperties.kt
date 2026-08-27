package avh.ckc.demo.config

import avh.ckc.core.ProcessingMode
import avh.ckc.demo.DemoTopics
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "demo")
data class DemoApplicationProperties(
    var experimentTargetName: String = "",
    var kafka: Kafka = Kafka(),
    var topics: Topics = Topics(),
    var model: Model = Model(),
    var registry: Registry = Registry(),
    var audit: Audit = Audit(),
    var consumers: Consumers = Consumers()
) {
    data class Kafka(
        var enabled: Boolean = false,
        var bootstrapServers: String = "localhost:9092",
        var groupId: String = "ckc-demo",
        var consumer: KafkaConsumer = KafkaConsumer()
    )

    data class KafkaConsumer(
        var fetchMinBytes: Int = 1,
        var fetchMaxWaitMs: Int = 500,
        var maxPollRecords: Int = 500,
        var fetchMaxBytes: Int = 50 * 1024 * 1024,
        var maxPartitionFetchBytes: Int = 1024 * 1024,
        var commitIntervalMs: Int = 5_000
    )

    data class KafkaConsumerOverrides(
        var fetchMinBytes: Int? = null,
        var fetchMaxWaitMs: Int? = null,
        var maxPollRecords: Int? = null,
        var fetchMaxBytes: Int? = null,
        var maxPartitionFetchBytes: Int? = null
    )

    data class Topics(
        var orderEvents: String = DemoTopics.ORDER_EVENTS,
        var batchEvents: String = DemoTopics.BATCH_EVENTS,
        var cauldronEvents: String = DemoTopics.CAULDRON_EVENTS
    )

    data class Model(
        var baseUrl: String = "http://127.0.0.1:18080",
        var etaBaseUrl: String = "",
        var flavourBaseUrl: String = "",
        var httpClient: ModelHttpClient = ModelHttpClient.ARMERIA,
        var syncHttpClient: ModelHttpClient = ModelHttpClient.ARMERIA,
        var jdkHttpClientExecutor: JdkHttpClientExecutor = JdkHttpClientExecutor.DEFAULT,
        var jdkHttpClientVirtualThreadNamePrefix: String = "HttpClient-virtual-"
    )

    data class Registry(
        var baseUrl: String = "http://127.0.0.1:18080"
    )

    data class Audit(
        var enabled: Boolean = true,
        var host: String = "127.0.0.1",
        var port: Int = 5170,
        var runId: String = "local",
        var writerId: String = "demo"
    )

    data class Consumers(
        var processingEnabled: Boolean = true,
        var metricsImplementation: MetricsImplementation = MetricsImplementation.MICROMETER,
        var processingDispatcherType: ProcessingDispatcherType = ProcessingDispatcherType.AUTO,
        var workerDispatcherThreads: Int = 8,
        var virtualThreadNamePrefix: String = "demo-processing-virtual-",
        var freshnessFirstMaxRecordAgeSeconds: Long = 10,
        var retry: Retry = Retry(),
        var order: ConsumerRuntime = ConsumerRuntime(workerConcurrency = 2, workChannelCapacity = 1024),
        var batch: ConsumerRuntime = ConsumerRuntime(workerConcurrency = 2, workChannelCapacity = 1024),
        var telemetry: ConsumerRuntime = ConsumerRuntime(
            workerConcurrency = 4,
            workChannelCapacity = 256,
            processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST
        )
    )

    enum class MetricsImplementation {
        MICROMETER,
        NOOP
    }

    enum class JdkHttpClientExecutor {
        DEFAULT,
        VIRTUAL
    }

    enum class ModelHttpClient {
        ARMERIA,
        JDK
    }

    enum class ProcessingDispatcherType {
        AUTO,
        DEFAULT,
        IO,
        FIXED,
        VIRTUAL
    }

    data class Retry(
        var maxAttempts: Int = 3,
        var backoffMs: Long = 250
    ) {
        init {
            require(maxAttempts >= 1) { "demo.consumers.retry.max-attempts must be >= 1" }
            require(backoffMs >= 0) { "demo.consumers.retry.backoff-ms must be >= 0" }
        }

        val maxRetries: Int
            get() = maxAttempts - 1
    }

    data class ConsumerRuntime(
        var workerConcurrency: Int = 1,
        var pollLoopConcurrency: Int = 1,
        var workChannelCapacity: Int = 1024,
        var processingMode: ProcessingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
        var kafka: KafkaConsumerOverrides = KafkaConsumerOverrides()
    )
}
