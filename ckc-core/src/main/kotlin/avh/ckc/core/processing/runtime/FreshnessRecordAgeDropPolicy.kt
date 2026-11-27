package avh.ckc.core.processing.runtime

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.RecordDropReason
import org.apache.kafka.clients.consumer.ConsumerRecord
import kotlin.time.Duration

internal class FreshnessRecordAgeDropPolicy<K, V>(
    private val maxRecordAge: Duration?,
    private val metrics: ConsumerMetrics<K, V>
) {
    fun shouldDrop(record: ConsumerRecord<K, V>, nowMillis: Long = System.currentTimeMillis()): Boolean {
        if (!isStaleRecord(record.timestamp(), maxRecordAge, nowMillis)) return false

        metrics.onRecordDropped(record, RecordDropReason.STALE_AGE)
        return true
    }
}

internal fun isStaleRecord(
    recordTimestamp: Long,
    maxRecordAge: Duration?,
    nowMillis: Long = System.currentTimeMillis()
): Boolean = maxRecordAge != null && recordTimestamp > 0L &&
    nowMillis - recordTimestamp > maxRecordAge.inWholeMilliseconds
