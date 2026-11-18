package avh.ckc.demo.consumer.ckc

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.CoroutinesKafkaConsumerBuilder
import avh.ckc.core.RetryPolicy
import avh.ckc.core.RetryRule
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.metrics.RecordHandoffReason
import avh.ckc.core.coroutinesKafkaConsumer
import avh.ckc.core.warmup.TrafficWarmupContext
import avh.ckc.core.warmup.TrafficWarmupForwardedEvent
import avh.ckc.demo.DemoTopics
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.logFailed
import avh.ckc.demo.logDropped
import avh.ckc.demo.logHandedOff
import avh.ckc.demo.logProcessed
import avh.ckc.demo.logRetryAttempt
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.serialization.BatchLifecycleEventDeserializer
import avh.ckc.demo.serialization.BatchLifecycleEventSerializer
import avh.ckc.demo.serialization.CauldronTelemetryEventDeserializer
import avh.ckc.demo.serialization.CauldronTelemetryEventSerializer
import avh.ckc.demo.serialization.OrderLifecycleEventDeserializer
import avh.ckc.demo.serialization.OrderLifecycleEventSerializer
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.delay
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

object DemoConsumers {
    private val log = LoggerFactory.getLogger(DemoConsumers::class.java)

    data class TrafficWarmup(
        val context: TrafficWarmupContext,
        val consumerProperties: Map<String, Any?>,
        val producerProperties: Map<String, Any?>,
        val configuration: DemoApplicationProperties.TrafficWarmup
    )

    fun orderConsumer(
        baseProperties: Map<String, Any>,
        metrics: ConsumerMetrics<String, OrderLifecycleEvent>,
        audit: DemoApplicationProperties.Audit,
        runtime: DemoApplicationProperties.ConsumerRuntime,
        retry: DemoApplicationProperties.Retry,
        processingDispatcher: CoroutineDispatcher,
        processingEnabled: Boolean,
        trafficWarmup: TrafficWarmup? = null,
        handler: suspend (String?, OrderLifecycleEvent) -> Unit
    ): CoroutinesKafkaConsumer<String, OrderLifecycleEvent> {
        val properties = baseProperties + mapOf(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to OrderLifecycleEventDeserializer::class.java,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false
        )

        return coroutinesKafkaConsumer(properties) {
            topics(DemoTopics.ORDER_EVENTS)
            processingMode = runtime.processingMode
            workerConcurrency = runtime.workerConcurrency
            consumerPollLoopConcurrency = runtime.pollLoopConcurrency
            workChannelCapacity = runtime.workChannelCapacity
            this.processingDispatcher = processingDispatcher
            retryPolicy = demoRetryPolicy(retry)
            this.metrics = auditMetrics(metrics, audit)
            configureTrafficWarmup(
                trafficWarmup,
                quota = trafficWarmup?.configuration?.mappings?.orderEvents,
                participantId = "order-events",
                sourceTopic = DemoTopics.ORDER_EVENTS,
                warmupTopic = DemoTopics.WARMUP_ORDER_EVENTS,
                valueSerializer = OrderLifecycleEventSerializer::class.java
            )
            onProcessingFailure { record, _ ->
                logFailed(record, audit)
            }
            handle { record ->
                val value = record.value()
                if (value != null) {
                    if (processingEnabled) {
                        handler(record.key(), value)
                    } else {
                        latencyOnlyDelay()
                    }
                }
            }
        }
    }

    fun batchConsumer(
        baseProperties: Map<String, Any>,
        metrics: ConsumerMetrics<String, BatchLifecycleEvent>,
        audit: DemoApplicationProperties.Audit,
        runtime: DemoApplicationProperties.ConsumerRuntime,
        retry: DemoApplicationProperties.Retry,
        processingDispatcher: CoroutineDispatcher,
        processingEnabled: Boolean,
        trafficWarmup: TrafficWarmup? = null,
        handler: suspend (String?, BatchLifecycleEvent) -> Unit
    ): CoroutinesKafkaConsumer<String, BatchLifecycleEvent> {
        val properties = baseProperties + mapOf(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to BatchLifecycleEventDeserializer::class.java,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false
        )

        return coroutinesKafkaConsumer(properties) {
            topics(DemoTopics.BATCH_EVENTS)
            processingMode = runtime.processingMode
            workerConcurrency = runtime.workerConcurrency
            consumerPollLoopConcurrency = runtime.pollLoopConcurrency
            workChannelCapacity = runtime.workChannelCapacity
            this.processingDispatcher = processingDispatcher
            retryPolicy = demoRetryPolicy(retry)
            this.metrics = auditMetrics(metrics, audit)
            configureTrafficWarmup(
                trafficWarmup,
                quota = trafficWarmup?.configuration?.mappings?.batchEvents,
                participantId = "batch-events",
                sourceTopic = DemoTopics.BATCH_EVENTS,
                warmupTopic = DemoTopics.WARMUP_BATCH_EVENTS,
                valueSerializer = BatchLifecycleEventSerializer::class.java
            )
            onProcessingFailure { record, _ ->
                logFailed(record, audit)
            }
            handle { record ->
                val value = record.value()
                if (value != null) {
                    if (processingEnabled) {
                        handler(record.key(), value)
                    } else {
                        latencyOnlyDelay()
                    }
                }
            }
        }
    }

    fun telemetryConsumer(
        baseProperties: Map<String, Any>,
        metrics: ConsumerMetrics<String, CauldronTelemetryEvent>,
        audit: DemoApplicationProperties.Audit,
        runtime: DemoApplicationProperties.ConsumerRuntime,
        freshnessMaxRecordAgeSeconds: Long,
        retry: DemoApplicationProperties.Retry,
        processingDispatcher: CoroutineDispatcher,
        processingEnabled: Boolean,
        trafficWarmup: TrafficWarmup? = null,
        handler: suspend (String?, CauldronTelemetryEvent) -> Unit
    ): CoroutinesKafkaConsumer<String, CauldronTelemetryEvent> {
        val properties = baseProperties + mapOf(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to CauldronTelemetryEventDeserializer::class.java,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false
        )

        return coroutinesKafkaConsumer(properties) {
            topics(DemoTopics.CAULDRON_EVENTS)
            processingMode = runtime.processingMode
            workerConcurrency = runtime.workerConcurrency
            consumerPollLoopConcurrency = runtime.pollLoopConcurrency
            workChannelCapacity = runtime.workChannelCapacity
            freshnessMaxRecordAge = freshnessMaxRecordAgeSeconds
                .takeIf { it > 0 }
                ?.seconds
            this.processingDispatcher = processingDispatcher
            retryPolicy = demoRetryPolicy(retry)
            this.metrics = auditMetrics(metrics, audit)
            configureTrafficWarmup(
                trafficWarmup,
                quota = trafficWarmup?.configuration?.mappings?.cauldronEvents,
                participantId = "cauldron-events",
                sourceTopic = DemoTopics.CAULDRON_EVENTS,
                warmupTopic = DemoTopics.WARMUP_CAULDRON_EVENTS,
                valueSerializer = CauldronTelemetryEventSerializer::class.java
            )
            onProcessingFailure { record, _ ->
                logFailed(record, audit)
            }
            handle { record ->
                val value = record.value()
                if (value != null) {
                    if (processingEnabled) {
                        handler(record.key(), value)
                    } else {
                        latencyOnlyDelay()
                    }
                }
            }
        }
    }

    private suspend fun latencyOnlyDelay() {
        delay((5L..8L).random())
    }

    private fun demoRetryPolicy(retry: DemoApplicationProperties.Retry): RetryPolicy =
        RetryPolicy.of(
            RetryRule.of(
                exceptionTypes = listOf(Exception::class),
                maxRetries = retry.maxRetries,
                delay = retry.backoffMs.milliseconds
            )
        )

    internal fun <K, V> auditMetrics(
        delegate: ConsumerMetrics<K, V>,
        audit: DemoApplicationProperties.Audit
    ): ConsumerMetrics<K, V> {
        return object : ConsumerMetrics<K, V> by delegate {
            override fun runtimeBinding(bindingId: String): ConsumerMetrics<K, V> =
                auditMetrics(delegate.runtimeBinding(bindingId), audit)

            override fun onRecordProcessed(
                key: K?,
                value: V?,
                record: ConsumerRecord<K, V>,
                endToEndLatencyMillis: Long,
                durationNanos: Long
            ) = onRecordProcessed(
                key,
                value,
                record,
                endToEndLatencyMillis,
                durationNanos,
                processedRemotely = false
            )

            override fun onRecordProcessed(
                key: K?,
                value: V?,
                record: ConsumerRecord<K, V>,
                endToEndLatencyMillis: Long,
                durationNanos: Long,
                processedRemotely: Boolean
            ) {
                delegate.onRecordProcessed(
                    key,
                    value,
                    record,
                    endToEndLatencyMillis,
                    durationNanos,
                    processedRemotely
                )
                logProcessed(record, audit)
            }

            override fun onRetry(key: K?, value: V?, record: ConsumerRecord<K, V>, attempt: Int, error: Throwable) {
                delegate.onRetry(key, value, record, attempt, error)
                logRetryAttempt(record, audit)
            }

            override fun onRecordDropped(record: ConsumerRecord<K, V>, reason: RecordDropReason) {
                delegate.onRecordDropped(record, reason)
                logDropped(record, audit, reason.auditReason())
            }

            override fun onRecordHandedOff(record: ConsumerRecord<K, V>, reason: RecordHandoffReason) {
                delegate.onRecordHandedOff(record, reason)
                logHandedOff(record, audit, reason.name.lowercase())
            }
        }
    }

    private fun RecordDropReason.auditReason(): String = name.lowercase()

    private fun <V> CoroutinesKafkaConsumerBuilder<String, V>.configureTrafficWarmup(
        warmup: TrafficWarmup?,
        quota: DemoApplicationProperties.TrafficWarmupMapping?,
        participantId: String,
        sourceTopic: String,
        warmupTopic: String,
        valueSerializer: Class<*>
    ) {
        if (warmup == null) return
        requireNotNull(quota) { "Warm-up quota is required for participant '$participantId'" }
        trafficWarmup(warmup.context, participantId) {
            mapTopic(sourceTopic, warmupTopic) {
                quotaProfile = quota.quotaProfile
                quotaInterval = quota.quotaIntervalMs.milliseconds
            }
            warmupConsumerProperties = warmup.consumerProperties
            forwardingProducerProperties = warmup.producerProperties + mapOf(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to valueSerializer
            )
            onForwarded(::logForwarded)
        }
    }

    private fun logForwarded(event: TrafficWarmupForwardedEvent) {
        log.info(
            "Warm-up record forwarded: sessionId={}, source={}-{}@{}, destination={}-{}@{}",
            event.sessionId,
            event.sourceTopic,
            event.sourcePartition,
            event.sourceOffset,
            event.destinationTopic,
            event.destinationPartition,
            event.destinationOffset
        )
    }
}
