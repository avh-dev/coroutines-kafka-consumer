package avh.ckc.demo.config

import avh.ckc.demo.DemoTopics
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "demo")
data class DemoApplicationProperties(
    var kafka: Kafka = Kafka(),
    var topics: Topics = Topics(),
    var model: Model = Model()
) {
    data class Kafka(
        var enabled: Boolean = false,
        var bootstrapServers: String = "localhost:9092",
        var lifecycleGroupId: String = "potion-tracking-lifecycle",
        var telemetryGroupId: String = "potion-tracking-telemetry"
    )

    data class Topics(
        var orderLifecycle: String = DemoTopics.ORDER_LIFECYCLE,
        var cauldronTelemetry: String = DemoTopics.CAULDRON_TELEMETRY
    )

    data class Model(
        var baseUrl: String = "http://127.0.0.1:18080"
    )
}
