package avh.ckc.demo.config

import avh.ckc.core.ProcessingMode
import avh.ckc.demo.DemoTopics
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "demo")
data class DemoApplicationProperties(
    var kafka: Kafka = Kafka(),
    var topics: Topics = Topics(),
    var model: Model = Model(),
    var audit: Audit = Audit(),
    var consumers: Consumers = Consumers()
) {
    data class Kafka(
        var enabled: Boolean = false,
        var bootstrapServers: String = "localhost:9092",
        var orderGroupId: String = "potion-tracking-orders",
        var batchGroupId: String = "potion-tracking-batches",
        var cauldronGroupId: String = "potion-tracking-cauldrons",
        var consumer: KafkaConsumer = KafkaConsumer()
    )

    data class KafkaConsumer(
        var fetchMinBytes: Int = 1,
        var fetchMaxWaitMs: Int = 500,
        var maxPollRecords: Int = 500
    )

    data class Topics(
        var orderEvents: String = DemoTopics.ORDER_EVENTS,
        var batchEvents: String = DemoTopics.BATCH_EVENTS,
        var cauldronEvents: String = DemoTopics.CAULDRON_EVENTS
    )

    data class Model(
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
        var workerDispatcherThreads: Int = 8,
        var freshnessFirstMaxRecordAgeSeconds: Long = 10,
        var order: ConsumerRuntime = ConsumerRuntime(workerConcurrency = 2, workChannelCapacity = 1024),
        var batch: ConsumerRuntime = ConsumerRuntime(workerConcurrency = 2, workChannelCapacity = 1024),
        var telemetry: ConsumerRuntime = ConsumerRuntime(
            workerConcurrency = 4,
            workChannelCapacity = 256,
            processingMode = ProcessingMode.FRESHNESS_FIRST
        )
    )

    enum class MetricsImplementation {
        MICROMETER,
        NOOP
    }

    data class ConsumerRuntime(
        var workerConcurrency: Int = 1,
        var pollLoopConcurrency: Int = 1,
        var workChannelCapacity: Int = 1024,
        var processingMode: ProcessingMode = ProcessingMode.AT_LEAST_ONCE_UNORDERED
    )
}
