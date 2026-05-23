package avh.ckc.demo.config

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.micrometer.MicrometerConsumerMetrics
import avh.ckc.micrometer.consumerRecordTagValueProvider
import avh.ckc.micrometer.recordMetricTag
import avh.ckc.micrometer.recordMetricTagSchema
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.core.env.Environment
import org.springframework.core.env.Profiles

@Configuration(proxyBeanMethods = false)
class MetricsConfiguration {
    private val eventTypeTag = recordMetricTag("event_type")

    @Bean
    @Profile("ckc")
    fun micrometerConsumerMetrics(meterRegistry: MeterRegistry): MicrometerConsumerMetrics =
        MicrometerConsumerMetrics(
            meterRegistry = meterRegistry,
            recordTagSchema = recordMetricTagSchema(eventTypeTag)
        )

    @Bean
    @Profile("spring-kafka")
    fun springKafkaMicrometerConsumerMetrics(meterRegistry: MeterRegistry): MicrometerConsumerMetrics =
        MicrometerConsumerMetrics(
            meterRegistry = meterRegistry,
            recordTagSchema = recordMetricTagSchema(eventTypeTag)
        )

    @Bean
    fun consumerProfileInfoMetric(
        meterRegistry: MeterRegistry,
        environment: Environment
    ): Gauge {
        val profile = activeConsumerProfile(environment)
        return Gauge.builder("ckc.demo.consumer.profile.info") { 1.0 }
            .description("Static marker for the active demo consumer implementation.")
            .tag("consumer_impl", consumerImplementation(profile))
            .tag("spring_profile", profile)
            .register(meterRegistry)
    }

    @Bean
    @Profile("ckc")
    fun consumerMetrics(
        @Qualifier("micrometerConsumerMetrics") micrometerConsumerMetrics: MicrometerConsumerMetrics
    ): ConsumerMetrics<String, CauldronTelemetryEvent> =
        micrometerConsumerMetrics.forConsumer(
            consumerId = "cauldron_events",
            cauldronTelemetryTagValueProvider()
        )

    @Bean
    @Profile("ckc")
    fun orderConsumerMetrics(
        @Qualifier("micrometerConsumerMetrics") micrometerConsumerMetrics: MicrometerConsumerMetrics
    ): ConsumerMetrics<String, OrderLifecycleEvent> =
        micrometerConsumerMetrics.forConsumer(
            consumerId = "order_events",
            orderLifecycleTagValueProvider()
        )

    @Bean
    @Profile("ckc")
    fun batchConsumerMetrics(
        @Qualifier("micrometerConsumerMetrics") micrometerConsumerMetrics: MicrometerConsumerMetrics
    ): ConsumerMetrics<String, BatchLifecycleEvent> =
        micrometerConsumerMetrics.forConsumer(
            consumerId = "batch_events",
            batchLifecycleTagValueProvider()
        )

    @Bean
    @Profile("spring-kafka")
    fun springKafkaConsumerMetrics(
        @Qualifier("springKafkaMicrometerConsumerMetrics") micrometerConsumerMetrics: MicrometerConsumerMetrics
    ): ConsumerMetrics<String, CauldronTelemetryEvent> =
        micrometerConsumerMetrics.forConsumer(
            consumerId = "cauldron_events",
            cauldronTelemetryTagValueProvider()
        )

    @Bean
    @Profile("spring-kafka")
    fun springKafkaOrderConsumerMetrics(
        @Qualifier("springKafkaMicrometerConsumerMetrics") micrometerConsumerMetrics: MicrometerConsumerMetrics
    ): ConsumerMetrics<String, OrderLifecycleEvent> =
        micrometerConsumerMetrics.forConsumer(
            consumerId = "order_events",
            orderLifecycleTagValueProvider()
        )

    @Bean
    @Profile("spring-kafka")
    fun springKafkaBatchConsumerMetrics(
        @Qualifier("springKafkaMicrometerConsumerMetrics") micrometerConsumerMetrics: MicrometerConsumerMetrics
    ): ConsumerMetrics<String, BatchLifecycleEvent> =
        micrometerConsumerMetrics.forConsumer(
            consumerId = "batch_events",
            batchLifecycleTagValueProvider()
        )

    private fun cauldronTelemetryTagValueProvider() =
        consumerRecordTagValueProvider<String, CauldronTelemetryEvent> { _, _, _ ->
            set(eventTypeTag, "CAULDRON_TELEMETRY")
        }

    private fun orderLifecycleTagValueProvider() =
        consumerRecordTagValueProvider<String, OrderLifecycleEvent> { _, event, _ ->
            set(eventTypeTag, event?.eventType?.name ?: "UNKNOWN")
        }

    private fun batchLifecycleTagValueProvider() =
        consumerRecordTagValueProvider<String, BatchLifecycleEvent> { _, event, _ ->
            set(eventTypeTag, event?.eventType?.name ?: "UNKNOWN")
        }

    private fun activeConsumerProfile(environment: Environment): String =
        when {
            environment.acceptsProfiles(Profiles.of("confluent-parallel")) -> "confluent-parallel"
            environment.acceptsProfiles(Profiles.of("spring-kafka")) -> "spring-kafka"
            environment.acceptsProfiles(Profiles.of("ckc")) -> "ckc"
            else -> environment.activeProfiles.firstOrNull() ?: "unknown"
        }

    private fun consumerImplementation(profile: String): String =
        when (profile) {
            "confluent-parallel" -> "confluent_parallel"
            "spring-kafka" -> "spring_kafka"
            "ckc" -> "ckc"
            else -> "unknown"
        }
}
