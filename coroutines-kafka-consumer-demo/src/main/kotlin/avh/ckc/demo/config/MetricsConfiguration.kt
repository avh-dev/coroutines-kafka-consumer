package avh.ckc.demo.config

import avh.ckc.core.ConsumerTelemetry
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.micrometer.MicrometerConsumerTelemetry
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
    fun micrometerConsumerTelemetry(meterRegistry: MeterRegistry): MicrometerConsumerTelemetry =
        MicrometerConsumerTelemetry(
            meterRegistry = meterRegistry,
            recordTagSchema = recordMetricTagSchema(eventTypeTag)
        )

    @Bean
    fun consumerTelemetry(micrometerConsumerTelemetry: MicrometerConsumerTelemetry): ConsumerTelemetry<String, CauldronTelemetryEvent> =
        micrometerConsumerTelemetry.forConsumer(
            consumerRecordTagValueProvider<String, CauldronTelemetryEvent> { _, _, _ ->
                set(eventTypeTag, "CAULDRON_TELEMETRY")
            }
        )

    @Bean
    fun lifecycleConsumerTelemetry(micrometerConsumerTelemetry: MicrometerConsumerTelemetry): ConsumerTelemetry<String, OrderLifecycleEvent> =
        micrometerConsumerTelemetry.forConsumer(
            consumerRecordTagValueProvider<String, OrderLifecycleEvent> { _, event, _ ->
                set(eventTypeTag, event?.eventType?.name ?: "UNKNOWN")
            }
        )
}
