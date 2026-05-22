package avh.ckc.core.processing

import avh.ckc.core.polling.partition.PartitionRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord

internal interface ProcessedRecordTracker {
    fun isProcessed(record: ConsumerRecord<ByteArray, ByteArray>): Boolean

    fun markProcessed(record: ConsumerRecord<ByteArray, ByteArray>)
}

internal object NoopProcessedRecordTracker : ProcessedRecordTracker {
    override fun isProcessed(record: ConsumerRecord<ByteArray, ByteArray>): Boolean = false

    override fun markProcessed(record: ConsumerRecord<ByteArray, ByteArray>) = Unit
}

internal class PartitionProcessedRecordTracker(
    private val partitionRegistry: PartitionRegistry
) : ProcessedRecordTracker {
    override fun isProcessed(record: ConsumerRecord<ByteArray, ByteArray>): Boolean =
        partitionRegistry.partitionStateFor(record)?.isProcessed(record.offset()) == true

    override fun markProcessed(record: ConsumerRecord<ByteArray, ByteArray>) {
        partitionRegistry.partitionStateFor(record)?.markProcessed(record.offset())
    }
}
