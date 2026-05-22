package avh.ckc.core.processing.runtime

import avh.ckc.core.KafkaRecordHandler
import avh.ckc.core.ProcessingFailureHandler
import avh.ckc.core.RetryPolicy
import avh.ckc.core.processing.deserialization.RecordDeserializerFactory
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.ConsumerRuntimeStatsTracker
import avh.ckc.core.processing.ProcessedRecordTracker
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import org.apache.kafka.clients.consumer.ConsumerRecord

internal class AtLeastOnceUnorderedRecordProcessingRuntime<K, V>(
    workerConcurrency: Int,
    workChannelCapacity: Int,
    processingDispatcher: CoroutineDispatcher,
    scope: CoroutineScope,
    metrics: ConsumerMetrics<K, V>,
    recordDeserializerFactory: RecordDeserializerFactory<K, V>,
    handler: KafkaRecordHandler<K, V>,
    retryPolicy: RetryPolicy,
    processingFailureHandler: ProcessingFailureHandler<K, V>,
    processedRecordTracker: ProcessedRecordTracker
) : BaseUnorderedRecordProcessingRuntime<K, V>(
    workerConcurrency = workerConcurrency,
    workChannelCapacity = workChannelCapacity,
    processingDispatcher = processingDispatcher,
    scope = scope,
    metrics = metrics,
    recordDeserializerFactory = recordDeserializerFactory,
    handler = handler,
    retryPolicy = retryPolicy,
    processingFailureHandler = processingFailureHandler,
    processedRecordTracker = processedRecordTracker
) {
    override fun createChannel(
        capacity: Int,
        runtimeStats: ConsumerRuntimeStatsTracker
    ): Channel<ConsumerRecord<ByteArray, ByteArray>> =
        Channel(
            capacity = capacity,
            onBufferOverflow = BufferOverflow.SUSPEND,
            onUndeliveredElement = {
                runtimeStats.onWorkDequeued()
            }
        )
}
