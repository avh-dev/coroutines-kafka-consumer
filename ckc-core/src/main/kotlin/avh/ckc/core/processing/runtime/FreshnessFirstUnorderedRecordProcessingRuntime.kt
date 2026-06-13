package avh.ckc.core.processing.runtime

import avh.ckc.core.KafkaRecordHandler
import avh.ckc.core.ProcessingFailureHandler
import avh.ckc.core.RetryPolicy
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.ConsumerRuntimeStatsTracker
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.processing.ProcessedRecordTracker
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.concurrent.atomic.AtomicBoolean

internal class FreshnessFirstUnorderedRecordProcessingRuntime<K, V>(
    workerConcurrency: Int,
    workChannelCapacity: Int,
    processingDispatcher: CoroutineDispatcher,
    scope: CoroutineScope,
    metrics: ConsumerMetrics<K, V>,
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
    handler = handler,
    retryPolicy = retryPolicy,
    processingFailureHandler = processingFailureHandler,
    processedRecordTracker = processedRecordTracker
) {
    private val acceptingRecords = AtomicBoolean(true)

    override fun createChannel(
        capacity: Int,
        runtimeStats: ConsumerRuntimeStatsTracker
    ): Channel<ConsumerRecord<K, V>> =
        Channel(
            capacity = capacity,
            onBufferOverflow = BufferOverflow.DROP_OLDEST,
            onUndeliveredElement = { record ->
                runtimeStats.onWorkDequeued()
                if (acceptingRecords.get()) {
                    metrics.onRecordDropped(record, RecordDropReason.QUEUE_OVERFLOW)
                }
            }
        )

    override fun close(cause: Throwable?) {
        acceptingRecords.set(false)
        super.close(cause)
    }

    override suspend fun stop() {
        acceptingRecords.set(false)
        super.stop()
    }
}
