package avh.ckc.core.processing

import avh.ckc.core.polling.partition.PartitionRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlin.time.Duration

internal interface ProcessedRecordTracker {
    fun <K, V> isProcessed(record: ConsumerRecord<K, V>): Boolean

    fun <K, V> markProcessed(record: ConsumerRecord<K, V>)

    fun <K, V> tryStart(record: ConsumerRecord<K, V>): Boolean = true

    fun <K, V> finish(record: ConsumerRecord<K, V>) = Unit

    /** Releases ownership bookkeeping when the record leaves a queue without ever starting. */
    fun <K, V> discard(record: ConsumerRecord<K, V>) = Unit

    fun <K, V> attach(record: ConsumerRecord<K, V>, job: Job) = Unit

    fun <K, V> isCancellationRequested(record: ConsumerRecord<K, V>): Boolean = false

    /** Waits for retry backoff and confirms that the next handler attempt may start. */
    suspend fun <K, V> awaitRetryPermission(record: ConsumerRecord<K, V>, retryDelay: Duration): Boolean {
        if (retryDelay.isPositive()) delay(retryDelay)
        return true
    }
}

internal object NoopProcessedRecordTracker : ProcessedRecordTracker {
    override fun <K, V> isProcessed(record: ConsumerRecord<K, V>): Boolean = false

    override fun <K, V> markProcessed(record: ConsumerRecord<K, V>) = Unit
}

internal class PartitionProcessedRecordTracker(
    private val partitionRegistry: PartitionRegistry,
    private val trackOffsets: Boolean = true
) : ProcessedRecordTracker {
    override fun <K, V> isProcessed(record: ConsumerRecord<K, V>): Boolean =
        trackOffsets && partitionRegistry.partitionStateFor(record)?.isProcessed(record.offset()) == true

    override fun <K, V> markProcessed(record: ConsumerRecord<K, V>) {
        // A stale completion must never advance a newly assigned generation.
        if (trackOffsets) partitionRegistry.markProcessed(record)
    }

    override fun <K, V> tryStart(record: ConsumerRecord<K, V>): Boolean = partitionRegistry.tryStart(record)

    override fun <K, V> finish(record: ConsumerRecord<K, V>) = partitionRegistry.finish(record)

    override fun <K, V> discard(record: ConsumerRecord<K, V>) = partitionRegistry.discard(record)

    override fun <K, V> attach(record: ConsumerRecord<K, V>, job: Job) = partitionRegistry.attach(record, job)

    override fun <K, V> isCancellationRequested(record: ConsumerRecord<K, V>): Boolean =
        partitionRegistry.isCancellationRequested(record)

    override suspend fun <K, V> awaitRetryPermission(
        record: ConsumerRecord<K, V>,
        retryDelay: Duration
    ): Boolean = if (trackOffsets) {
        partitionRegistry.awaitRetryPermission(record, retryDelay)
    } else {
        if (retryDelay.isPositive()) delay(retryDelay)
        true
    }
}
