package avh.ckc.demo.config

import avh.ckc.core.ConsumerMetrics
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.micrometer.MicrometerConsumerMetrics
import avh.ckc.micrometer.consumerRecordTagValueProvider
import avh.ckc.micrometer.recordMetricTag
import avh.ckc.micrometer.recordMetricTagSchema
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration(proxyBeanMethods = false)
class MetricsConfiguration {
    private val eventTypeTag = recordMetricTag("event_type")

    @Bean
    fun micrometerConsumerMetrics(meterRegistry: MeterRegistry): MicrometerConsumerMetrics =
        MicrometerConsumerMetrics(
            meterRegistry = meterRegistry,
            commonTags = listOf(Tag.of("consumer_impl", "ckc")),
            recordTagSchema = recordMetricTagSchema(eventTypeTag)
        )

    @Bean
    fun springKafkaMicrometerConsumerMetrics(meterRegistry: MeterRegistry): MicrometerConsumerMetrics =
        MicrometerConsumerMetrics(
            meterRegistry = meterRegistry,
            commonTags = listOf(Tag.of("consumer_impl", "spring_kafka")),
            recordTagSchema = recordMetricTagSchema(eventTypeTag)
        )

    @Bean
    fun consumerMetrics(
        @Qualifier("micrometerConsumerMetrics") micrometerConsumerMetrics: MicrometerConsumerMetrics
    ): ConsumerMetrics<String, CauldronTelemetryEvent> =
        micrometerConsumerMetrics.forConsumer(
            consumerId = "cauldron_telemetry",
            cauldronTelemetryTagValueProvider()
        )

    @Bean
    fun lifecycleConsumerMetrics(
        @Qualifier("micrometerConsumerMetrics") micrometerConsumerMetrics: MicrometerConsumerMetrics
    ): ConsumerMetrics<String, OrderLifecycleEvent> =
        micrometerConsumerMetrics.forConsumer(
            consumerId = "order_lifecycle",
            orderLifecycleTagValueProvider()
        )

    @Bean
    fun springKafkaConsumerMetrics(
        @Qualifier("springKafkaMicrometerConsumerMetrics") micrometerConsumerMetrics: MicrometerConsumerMetrics
    ): ConsumerMetrics<String, CauldronTelemetryEvent> =
        micrometerConsumerMetrics.forConsumer(
            consumerId = "cauldron_telemetry",
            cauldronTelemetryTagValueProvider()
        )

    @Bean
    fun springKafkaLifecycleConsumerMetrics(
        @Qualifier("springKafkaMicrometerConsumerMetrics") micrometerConsumerMetrics: MicrometerConsumerMetrics
    ): ConsumerMetrics<String, OrderLifecycleEvent> =
        micrometerConsumerMetrics.forConsumer(
            consumerId = "order_lifecycle",
            orderLifecycleTagValueProvider()
        )

    private fun cauldronTelemetryTagValueProvider() =
        consumerRecordTagValueProvider<String, CauldronTelemetryEvent> { _, _, _ ->
            set(eventTypeTag, "CAULDRON_TELEMETRY")
        }

    private fun orderLifecycleTagValueProvider() =
        consumerRecordTagValueProvider<String, OrderLifecycleEvent> { _, event, _ ->
            set(eventTypeTag, event?.eventType?.name ?: "UNKNOWN")
        }
}
