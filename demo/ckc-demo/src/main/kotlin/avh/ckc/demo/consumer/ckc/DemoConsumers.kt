package avh.ckc.demo.consumer.ckc

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.RetryPolicy
import avh.ckc.core.RetryRule
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.coroutinesKafkaConsumer
import avh.ckc.demo.DemoTopics
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.logFailed
import avh.ckc.demo.logDropped
import avh.ckc.demo.logProcessed
import avh.ckc.demo.logRetryAttempt
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.serialization.BatchLifecycleEventDeserializer
import avh.ckc.demo.serialization.CauldronTelemetryEventDeserializer
import avh.ckc.demo.serialization.OrderLifecycleEventDeserializer
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.delay
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import kotlin.time.Duration.Companion.milliseconds

object DemoConsumers {
    fun orderConsumer(
        baseProperties: Map<String, Any>,
        metrics: ConsumerMetrics<String, OrderLifecycleEvent>,
        audit: DemoApplicationProperties.Audit,
        runtime: DemoApplicationProperties.ConsumerRuntime,
        retry: DemoApplicationProperties.Retry,
        processingDispatcher: CoroutineDispatcher,
        processingEnabled: Boolean,
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
            this.metrics = metrics.withAudit(audit)
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
                    logProcessed(record, audit)
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
            this.metrics = metrics.withAudit(audit)
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
                    logProcessed(record, audit)
                }
            }
        }
    }

    fun telemetryConsumer(
        baseProperties: Map<String, Any>,
        metrics: ConsumerMetrics<String, CauldronTelemetryEvent>,
        audit: DemoApplicationProperties.Audit,
        runtime: DemoApplicationProperties.ConsumerRuntime,
        retry: DemoApplicationProperties.Retry,
        processingDispatcher: CoroutineDispatcher,
        processingEnabled: Boolean,
        handler: suspend (String?, CauldronTelemetryEvent) -> Unit
    ): CoroutinesKafkaConsumer<String, CauldronTelemetryEvent> {
        val properties = baseProperties + mapOf(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to CauldronTelemetryEventDeserializer::class.java,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to true
        )

        return coroutinesKafkaConsumer(properties) {
            topics(DemoTopics.CAULDRON_EVENTS)
            processingMode = runtime.processingMode
            workerConcurrency = runtime.workerConcurrency
            consumerPollLoopConcurrency = runtime.pollLoopConcurrency
            workChannelCapacity = runtime.workChannelCapacity
            this.processingDispatcher = processingDispatcher
            retryPolicy = demoRetryPolicy(retry)
            this.metrics = metrics.withAudit(audit)
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
                    logProcessed(record, audit)
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

    private fun <K, V> ConsumerMetrics<K, V>.withAudit(
        audit: DemoApplicationProperties.Audit
    ): ConsumerMetrics<K, V> {
        val delegate = this
        return object : ConsumerMetrics<K, V> by delegate {
            override fun onRetry(key: K?, value: V?, record: ConsumerRecord<K, V>, attempt: Int, error: Throwable) {
                delegate.onRetry(key, value, record, attempt, error)
                logRetryAttempt(record, audit)
            }

            override fun onRecordDropped(record: ConsumerRecord<K, V>, reason: RecordDropReason) {
                delegate.onRecordDropped(record, reason)
                logDropped(record, audit)
            }
        }
    }
}
