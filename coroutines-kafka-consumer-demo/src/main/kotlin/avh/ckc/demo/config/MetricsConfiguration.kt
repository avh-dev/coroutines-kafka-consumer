package avh.ckc.demo.config

import avh.ckc.core.ConsumerMetrics
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.micrometer.MicrometerConsumerMetrics
import avh.ckc.micrometer.consumerRecordTagValueProvider
import avh.ckc.micrometer.recordMetricTag
import avh.ckc.micrometer.recordMetricTagSchema
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration(proxyBeanMethods = false)
class MetricsConfiguration {
    private val eventTypeTag = recordMetricTag("event_type")

    @Bean
    fun micrometerConsumerMetrics(meterRegistry: MeterRegistry): MicrometerConsumerMetrics =
        MicrometerConsumerMetrics(
            meterRegistry = meterRegistry,
            recordTagSchema = recordMetricTagSchema(eventTypeTag)
        )

    @Bean
    fun consumerMetrics(micrometerConsumerMetrics: MicrometerConsumerMetrics): ConsumerMetrics<String, CauldronTelemetryEvent> =
        micrometerConsumerMetrics.forConsumer(
            consumerId = "cauldron_telemetry",
            consumerRecordTagValueProvider<String, CauldronTelemetryEvent> { _, _, _ ->
                set(eventTypeTag, "CAULDRON_TELEMETRY")
            }
        )

    @Bean
    fun lifecycleConsumerMetrics(micrometerConsumerMetrics: MicrometerConsumerMetrics): ConsumerMetrics<String, OrderLifecycleEvent> =
        micrometerConsumerMetrics.forConsumer(
            consumerId = "order_lifecycle",
            consumerRecordTagValueProvider<String, OrderLifecycleEvent> { _, event, _ ->
                set(eventTypeTag, event?.eventType?.name ?: "UNKNOWN")
            }
        )
}
