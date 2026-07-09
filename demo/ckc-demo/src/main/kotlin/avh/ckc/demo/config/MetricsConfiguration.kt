package avh.ckc.demo.config

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.micrometer.MicrometerConsumerMetricsSchema
import avh.ckc.micrometer.micrometerConsumerMetrics
import avh.ckc.micrometer.recordDrivenTags
import avh.ckc.micrometer.recordDrivenTagExtractors
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.core.env.Environment
import org.springframework.core.env.Profiles

@Configuration(proxyBeanMethods = false)
class MetricsConfiguration {
    private val eventTypeTag = "event_type"

    @Bean
    @Profile("ckc", "ckc-sync", "ckc-sync-loom", "ckc-spring-boot")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "MICROMETER", matchIfMissing = true)
    fun micrometerConsumerMetricsSchema(meterRegistry: MeterRegistry): MicrometerConsumerMetricsSchema =
        MicrometerConsumerMetricsSchema(
            meterRegistry = meterRegistry,
            metricPrefix = "demo",
            recordDrivenTags = recordDrivenTags(eventTypeTag)
        )

    @Bean
    @Profile("spring-kafka")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "MICROMETER", matchIfMissing = true)
    fun springKafkaMicrometerConsumerMetricsSchema(meterRegistry: MeterRegistry): MicrometerConsumerMetricsSchema =
        MicrometerConsumerMetricsSchema(
            meterRegistry = meterRegistry,
            metricPrefix = "demo",
            recordDrivenTags = recordDrivenTags(eventTypeTag)
        )

    @Bean
    @Profile("spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "MICROMETER", matchIfMissing = true)
    fun springKafkaCoroutinesNaiveMicrometerConsumerMetricsSchema(meterRegistry: MeterRegistry): MicrometerConsumerMetricsSchema =
        MicrometerConsumerMetricsSchema(
            meterRegistry = meterRegistry,
            metricPrefix = "demo",
            recordDrivenTags = recordDrivenTags(eventTypeTag)
        )

    @Bean
    @Profile("confluent-parallel", "confluent-parallel-reactor")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "MICROMETER", matchIfMissing = true)
    fun confluentParallelMicrometerConsumerMetricsSchema(meterRegistry: MeterRegistry): MicrometerConsumerMetricsSchema =
        MicrometerConsumerMetricsSchema(
            meterRegistry = meterRegistry,
            metricPrefix = "demo",
            recordDrivenTags = recordDrivenTags(eventTypeTag)
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
    @Profile("ckc", "ckc-sync", "ckc-sync-loom")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "MICROMETER", matchIfMissing = true)
    fun consumerMetrics(
        @Qualifier("micrometerConsumerMetricsSchema") micrometerConsumerMetricsSchema: MicrometerConsumerMetricsSchema
    ): ConsumerMetrics<String, CauldronTelemetryEvent> =
        micrometerConsumerMetrics(micrometerConsumerMetricsSchema) {
            consumerId = "cauldron_events"
            recordDrivenTagExtractors = cauldronTelemetryTagValueProvider()
        }

    @Bean
    @Profile("ckc", "ckc-sync", "ckc-sync-loom")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "MICROMETER", matchIfMissing = true)
    fun orderConsumerMetrics(
        @Qualifier("micrometerConsumerMetricsSchema") micrometerConsumerMetricsSchema: MicrometerConsumerMetricsSchema
    ): ConsumerMetrics<String, OrderLifecycleEvent> =
        micrometerConsumerMetrics(micrometerConsumerMetricsSchema) {
            consumerId = "order_events"
            recordDrivenTagExtractors = orderLifecycleTagValueProvider()
        }

    @Bean
    @Profile("ckc", "ckc-sync", "ckc-sync-loom")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "MICROMETER", matchIfMissing = true)
    fun batchConsumerMetrics(
        @Qualifier("micrometerConsumerMetricsSchema") micrometerConsumerMetricsSchema: MicrometerConsumerMetricsSchema
    ): ConsumerMetrics<String, BatchLifecycleEvent> =
        micrometerConsumerMetrics(micrometerConsumerMetricsSchema) {
            consumerId = "batch_events"
            recordDrivenTagExtractors = batchLifecycleTagValueProvider()
        }

    @Bean
    @Profile("spring-kafka")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "MICROMETER", matchIfMissing = true)
    fun springKafkaConsumerMetrics(
        @Qualifier("springKafkaMicrometerConsumerMetricsSchema") micrometerConsumerMetricsSchema: MicrometerConsumerMetricsSchema
    ): ConsumerMetrics<String, CauldronTelemetryEvent> =
        micrometerConsumerMetrics(micrometerConsumerMetricsSchema) {
            consumerId = "cauldron_events"
            recordDrivenTagExtractors = cauldronTelemetryTagValueProvider()
        }

    @Bean
    @Profile("spring-kafka")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "MICROMETER", matchIfMissing = true)
    fun springKafkaOrderConsumerMetrics(
        @Qualifier("springKafkaMicrometerConsumerMetricsSchema") micrometerConsumerMetricsSchema: MicrometerConsumerMetricsSchema
    ): ConsumerMetrics<String, OrderLifecycleEvent> =
        micrometerConsumerMetrics(micrometerConsumerMetricsSchema) {
            consumerId = "order_events"
            recordDrivenTagExtractors = orderLifecycleTagValueProvider()
        }

    @Bean
    @Profile("spring-kafka")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "MICROMETER", matchIfMissing = true)
    fun springKafkaBatchConsumerMetrics(
        @Qualifier("springKafkaMicrometerConsumerMetricsSchema") micrometerConsumerMetricsSchema: MicrometerConsumerMetricsSchema
    ): ConsumerMetrics<String, BatchLifecycleEvent> =
        micrometerConsumerMetrics(micrometerConsumerMetricsSchema) {
            consumerId = "batch_events"
            recordDrivenTagExtractors = batchLifecycleTagValueProvider()
        }

    @Bean
    @Profile("spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "MICROMETER", matchIfMissing = true)
    fun springKafkaCoroutinesNaiveConsumerMetrics(
        @Qualifier("springKafkaCoroutinesNaiveMicrometerConsumerMetricsSchema") micrometerConsumerMetricsSchema: MicrometerConsumerMetricsSchema
    ): ConsumerMetrics<String, CauldronTelemetryEvent> =
        micrometerConsumerMetrics(micrometerConsumerMetricsSchema) {
            consumerId = "cauldron_events"
            recordDrivenTagExtractors = cauldronTelemetryTagValueProvider()
        }

    @Bean
    @Profile("spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "MICROMETER", matchIfMissing = true)
    fun springKafkaCoroutinesNaiveOrderConsumerMetrics(
        @Qualifier("springKafkaCoroutinesNaiveMicrometerConsumerMetricsSchema") micrometerConsumerMetricsSchema: MicrometerConsumerMetricsSchema
    ): ConsumerMetrics<String, OrderLifecycleEvent> =
        micrometerConsumerMetrics(micrometerConsumerMetricsSchema) {
            consumerId = "order_events"
            recordDrivenTagExtractors = orderLifecycleTagValueProvider()
        }

    @Bean
    @Profile("spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "MICROMETER", matchIfMissing = true)
    fun springKafkaCoroutinesNaiveBatchConsumerMetrics(
        @Qualifier("springKafkaCoroutinesNaiveMicrometerConsumerMetricsSchema") micrometerConsumerMetricsSchema: MicrometerConsumerMetricsSchema
    ): ConsumerMetrics<String, BatchLifecycleEvent> =
        micrometerConsumerMetrics(micrometerConsumerMetricsSchema) {
            consumerId = "batch_events"
            recordDrivenTagExtractors = batchLifecycleTagValueProvider()
        }

    @Bean
    @Profile("confluent-parallel", "confluent-parallel-reactor")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "MICROMETER", matchIfMissing = true)
    fun confluentParallelConsumerMetrics(
        @Qualifier("confluentParallelMicrometerConsumerMetricsSchema") micrometerConsumerMetricsSchema: MicrometerConsumerMetricsSchema
    ): ConsumerMetrics<String, CauldronTelemetryEvent> =
        micrometerConsumerMetrics(micrometerConsumerMetricsSchema) {
            consumerId = "cauldron_events"
            recordDrivenTagExtractors = cauldronTelemetryTagValueProvider()
        }

    @Bean
    @Profile("confluent-parallel", "confluent-parallel-reactor")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "MICROMETER", matchIfMissing = true)
    fun confluentParallelOrderConsumerMetrics(
        @Qualifier("confluentParallelMicrometerConsumerMetricsSchema") micrometerConsumerMetricsSchema: MicrometerConsumerMetricsSchema
    ): ConsumerMetrics<String, OrderLifecycleEvent> =
        micrometerConsumerMetrics(micrometerConsumerMetricsSchema) {
            consumerId = "order_events"
            recordDrivenTagExtractors = orderLifecycleTagValueProvider()
        }

    @Bean
    @Profile("confluent-parallel", "confluent-parallel-reactor")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "MICROMETER", matchIfMissing = true)
    fun confluentParallelBatchConsumerMetrics(
        @Qualifier("confluentParallelMicrometerConsumerMetricsSchema") micrometerConsumerMetricsSchema: MicrometerConsumerMetricsSchema
    ): ConsumerMetrics<String, BatchLifecycleEvent> =
        micrometerConsumerMetrics(micrometerConsumerMetricsSchema) {
            consumerId = "batch_events"
            recordDrivenTagExtractors = batchLifecycleTagValueProvider()
        }

    @Bean("consumerMetrics")
    @Profile("ckc", "ckc-sync", "ckc-sync-loom")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "NOOP")
    fun noopTelemetryConsumerMetrics(): ConsumerMetrics<String, CauldronTelemetryEvent> = noopMetrics()

    @Bean("orderConsumerMetrics")
    @Profile("ckc", "ckc-sync", "ckc-sync-loom")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "NOOP")
    fun noopOrderConsumerMetrics(): ConsumerMetrics<String, OrderLifecycleEvent> = noopMetrics()

    @Bean("batchConsumerMetrics")
    @Profile("ckc", "ckc-sync", "ckc-sync-loom")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "NOOP")
    fun noopBatchConsumerMetrics(): ConsumerMetrics<String, BatchLifecycleEvent> = noopMetrics()

    @Bean("springKafkaConsumerMetrics")
    @Profile("spring-kafka")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "NOOP")
    fun noopSpringKafkaTelemetryConsumerMetrics(): ConsumerMetrics<String, CauldronTelemetryEvent> = noopMetrics()

    @Bean("springKafkaOrderConsumerMetrics")
    @Profile("spring-kafka")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "NOOP")
    fun noopSpringKafkaOrderConsumerMetrics(): ConsumerMetrics<String, OrderLifecycleEvent> = noopMetrics()

    @Bean("springKafkaBatchConsumerMetrics")
    @Profile("spring-kafka")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "NOOP")
    fun noopSpringKafkaBatchConsumerMetrics(): ConsumerMetrics<String, BatchLifecycleEvent> = noopMetrics()

    @Bean("springKafkaCoroutinesNaiveConsumerMetrics")
    @Profile("spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "NOOP")
    fun noopSpringKafkaCoroutinesNaiveTelemetryConsumerMetrics(): ConsumerMetrics<String, CauldronTelemetryEvent> =
        noopMetrics()

    @Bean("springKafkaCoroutinesNaiveOrderConsumerMetrics")
    @Profile("spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "NOOP")
    fun noopSpringKafkaCoroutinesNaiveOrderConsumerMetrics(): ConsumerMetrics<String, OrderLifecycleEvent> =
        noopMetrics()

    @Bean("springKafkaCoroutinesNaiveBatchConsumerMetrics")
    @Profile("spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "NOOP")
    fun noopSpringKafkaCoroutinesNaiveBatchConsumerMetrics(): ConsumerMetrics<String, BatchLifecycleEvent> =
        noopMetrics()

    @Bean("confluentParallelConsumerMetrics")
    @Profile("confluent-parallel", "confluent-parallel-reactor")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "NOOP")
    fun noopConfluentParallelTelemetryConsumerMetrics(): ConsumerMetrics<String, CauldronTelemetryEvent> =
        noopMetrics()

    @Bean("confluentParallelOrderConsumerMetrics")
    @Profile("confluent-parallel", "confluent-parallel-reactor")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "NOOP")
    fun noopConfluentParallelOrderConsumerMetrics(): ConsumerMetrics<String, OrderLifecycleEvent> =
        noopMetrics()

    @Bean("confluentParallelBatchConsumerMetrics")
    @Profile("confluent-parallel", "confluent-parallel-reactor")
    @ConditionalOnProperty(prefix = "demo.consumers", name = ["metrics-implementation"], havingValue = "NOOP")
    fun noopConfluentParallelBatchConsumerMetrics(): ConsumerMetrics<String, BatchLifecycleEvent> =
        noopMetrics()

    private fun cauldronTelemetryTagValueProvider() =
        recordDrivenTagExtractors<String, CauldronTelemetryEvent> {
            tag(eventTypeTag) { "CAULDRON_TELEMETRY" }
        }

    private fun orderLifecycleTagValueProvider() =
        recordDrivenTagExtractors<String, OrderLifecycleEvent> {
            tag(eventTypeTag) { record -> record.value()?.eventType?.name ?: "UNKNOWN" }
        }

    private fun batchLifecycleTagValueProvider() =
        recordDrivenTagExtractors<String, BatchLifecycleEvent> {
            tag(eventTypeTag) { record -> record.value()?.eventType?.name ?: "UNKNOWN" }
        }

    private fun activeConsumerProfile(environment: Environment): String =
        when {
            environment.acceptsProfiles(Profiles.of("confluent-parallel-reactor")) -> "confluent-parallel-reactor"
            environment.acceptsProfiles(Profiles.of("confluent-parallel")) -> "confluent-parallel"
            environment.acceptsProfiles(Profiles.of("spring-kafka-coroutines-naive")) -> "spring-kafka-coroutines-naive"
            environment.acceptsProfiles(Profiles.of("spring-kafka")) -> "spring-kafka"
            environment.acceptsProfiles(Profiles.of("ckc-sync-loom")) -> "ckc-sync-loom"
            environment.acceptsProfiles(Profiles.of("ckc-spring-boot")) -> "ckc-spring-boot"
            environment.acceptsProfiles(Profiles.of("ckc-sync")) -> "ckc-sync"
            environment.acceptsProfiles(Profiles.of("ckc")) -> "ckc"
            else -> environment.activeProfiles.firstOrNull() ?: "unknown"
        }

    private fun consumerImplementation(profile: String): String =
        when (profile) {
            "confluent-parallel", "confluent-parallel-reactor" -> "confluent_parallel"
            "spring-kafka-coroutines-naive" -> "spring_kafka_coroutines_naive"
            "spring-kafka" -> "spring_kafka"
            "ckc-sync", "ckc-sync-loom", "ckc-spring-boot" -> "ckc"
            "ckc" -> "ckc"
            else -> "unknown"
        }

    @Suppress("UNCHECKED_CAST")
    private fun <K, V> noopMetrics(): ConsumerMetrics<K, V> =
        ConsumerMetrics.NOOP as ConsumerMetrics<K, V>
}
