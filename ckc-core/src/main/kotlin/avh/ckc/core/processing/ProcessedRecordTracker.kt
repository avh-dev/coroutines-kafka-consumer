package avh.ckc.core.processing

import avh.ckc.core.polling.partition.PartitionRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord

internal interface ProcessedRecordTracker {
    fun <K, V> isProcessed(record: ConsumerRecord<K, V>): Boolean

    fun <K, V> markProcessed(record: ConsumerRecord<K, V>)
}

internal object NoopProcessedRecordTracker : ProcessedRecordTracker {
    override fun <K, V> isProcessed(record: ConsumerRecord<K, V>): Boolean = false

    override fun <K, V> markProcessed(record: ConsumerRecord<K, V>) = Unit
}

internal class PartitionProcessedRecordTracker(
    private val partitionRegistry: PartitionRegistry
) : ProcessedRecordTracker {
    override fun <K, V> isProcessed(record: ConsumerRecord<K, V>): Boolean =
        partitionRegistry.partitionStateFor(record)?.isProcessed(record.offset()) == true

    override fun <K, V> markProcessed(record: ConsumerRecord<K, V>) {
        partitionRegistry.partitionStateFor(record)?.markProcessed(record.offset())
    }
}
