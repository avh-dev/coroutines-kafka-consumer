package avh.ckc.demo.consumer.ckc

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.coroutinesKafkaConsumer
import avh.ckc.demo.DemoTopics
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.logFailed
import avh.ckc.demo.logProcessed
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.serialization.BatchLifecycleEventDeserializer
import avh.ckc.demo.serialization.CauldronTelemetryEventDeserializer
import avh.ckc.demo.serialization.OrderLifecycleEventDeserializer
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.delay
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

object DemoConsumers {
    fun orderConsumer(
        baseProperties: Map<String, Any>,
        metrics: ConsumerMetrics<String, OrderLifecycleEvent>,
        audit: DemoApplicationProperties.Audit,
        runtime: DemoApplicationProperties.ConsumerRuntime,
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
            this.metrics = metrics
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
            this.metrics = metrics
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
            this.metrics = metrics
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
}
