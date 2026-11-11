package avh.ckc.core.warmup

import avh.ckc.core.processing.AdmittedRecordInterceptor
import avh.ckc.core.processing.RecordProcessingOutcome
import org.apache.kafka.clients.consumer.ConsumerRecord

/** Keeps the normal consumer pipeline stable while a reviewed control session attaches/detaches delegation. */
internal class SwitchableWarmupDataPlane<K, V> : AdmittedRecordInterceptor<K, V>, AutoCloseable {
    private val lock = Any()
    private var active: FixedLinkWarmupDataPlane<K, V>? = null

    override suspend fun process(
        record: ConsumerRecord<K, V>,
        processLocally: suspend () -> RecordProcessingOutcome
    ): RecordProcessingOutcome = synchronized(lock) { active }?.process(record, processLocally) ?: processLocally()

    fun activate(dataPlane: FixedLinkWarmupDataPlane<K, V>) = synchronized(lock) {
        check(active == null) { "A warm-up data plane is already active" }
        active = dataPlane
    }

    fun accept(completed: WarmupRecordCompleted): WarmupResultTracker.Completion =
        synchronized(lock) { active }
            ?.takeIf { it.sessionId == completed.recordId.sessionId }
            ?.accept(completed)
            ?: WarmupResultTracker.Completion.IGNORED

    fun stopDelegating(sessionId: WarmupSessionId) {
        synchronized(lock) { active?.takeIf { it.sessionId == sessionId } }?.stopDelegating()
    }

    fun finish(sessionId: WarmupSessionId) {
        val completed = synchronized(lock) {
            active?.takeIf { it.sessionId == sessionId }?.also { active = null }
        }
        completed?.close()
    }

    override fun close() {
        val completed = synchronized(lock) { active.also { active = null } }
        completed?.close()
    }
}
