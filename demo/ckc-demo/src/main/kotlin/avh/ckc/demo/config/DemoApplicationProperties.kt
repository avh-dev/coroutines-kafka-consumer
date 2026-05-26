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
        var cauldronGroupId: String = "potion-tracking-cauldrons"
    )

    data class Topics(
        var orderEvents: String = DemoTopics.ORDER_EVENTS,
        var batchEvents: String = DemoTopics.BATCH_EVENTS,
        var cauldronEvents: String = DemoTopics.CAULDRON_EVENTS
    )

    data class Model(
        var baseUrl: String = "http://127.0.0.1:18080",
        var client: ModelClient = ModelClient.KTOR_CIO
    )

    enum class ModelClient {
        KTOR_CIO,
        ARMERIA
    }

    data class Audit(
        var enabled: Boolean = true
    )

    data class Consumers(
        var processingEnabled: Boolean = true,
        var deserializationDispatcher: DeserializationDispatcher = DeserializationDispatcher(),
        var order: ConsumerRuntime = ConsumerRuntime(workerConcurrency = 2, workChannelCapacity = 1024),
        var batch: ConsumerRuntime = ConsumerRuntime(workerConcurrency = 2, workChannelCapacity = 1024),
        var telemetry: ConsumerRuntime = ConsumerRuntime(
            workerConcurrency = 4,
            workChannelCapacity = 256,
            processingMode = ProcessingMode.FRESHNESS_FIRST
        )
    )

    data class DeserializationDispatcher(
        var mode: DeserializationDispatcherMode = DeserializationDispatcherMode.DEFAULT,
        var customThreadPoolSize: Int = 8,
        var customThreadNamePrefix: String = "ckc-demo-deserializer"
    )

    enum class DeserializationDispatcherMode {
        DEFAULT,
        IO,
        CUSTOM_THREAD_POOL
    }

    data class ConsumerRuntime(
        var workerConcurrency: Int = 1,
        var pollLoopConcurrency: Int = 1,
        var workChannelCapacity: Int = 1024,
        var processingMode: ProcessingMode = ProcessingMode.AT_LEAST_ONCE_UNORDERED
    )
}
