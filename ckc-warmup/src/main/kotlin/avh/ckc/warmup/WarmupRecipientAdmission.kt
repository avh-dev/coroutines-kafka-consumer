package avh.ckc.warmup

import avh.ckc.core.extension.RecordAdmission
import avh.ckc.warmup.protocol.WARMUP_SESSION_HEADER
import avh.ckc.warmup.protocol.WarmupSessionId
import org.apache.kafka.clients.consumer.ConsumerRecord

internal class WarmupRecipientAdmission<K, V>(
    sessionId: WarmupSessionId,
    private val resultAdmission: WarmupResultAdmission? = null
) : RecordAdmission<K, V> {
    private val expectedSessionHeader = sessionId.value.toByteArray(Charsets.UTF_8)

    override fun shouldSkip(record: ConsumerRecord<K, V>): Boolean = !belongsToRecipientSession(record)

    override fun tryAcquire(record: ConsumerRecord<K, V>): Boolean = resultAdmission?.tryAcquire(record) ?: true

    override fun release(record: ConsumerRecord<K, V>) {
        resultAdmission?.release(record)
    }

    private fun belongsToRecipientSession(record: ConsumerRecord<K, V>): Boolean {
        val headers = record.headers().headers(WARMUP_SESSION_HEADER).iterator()
        if (!headers.hasNext()) return false
        val value = headers.next().value()
        return !headers.hasNext() && value != null && value.contentEquals(expectedSessionHeader)
    }
}
