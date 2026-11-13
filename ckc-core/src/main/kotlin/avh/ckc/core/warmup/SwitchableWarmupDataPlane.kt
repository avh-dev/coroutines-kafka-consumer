package avh.ckc.core.warmup

import avh.ckc.core.processing.AdmittedRecordInterceptor
import avh.ckc.core.processing.RecordProcessingOutcome
import org.apache.kafka.clients.consumer.ConsumerRecord

/** Keeps the normal consumer pipeline stable while a reviewed control session attaches/detaches delegation. */
internal class SwitchableWarmupDataPlane<K, V> : AdmittedRecordInterceptor<K, V>, AutoCloseable {
    private val lock = Any()
    private val active = linkedMapOf<WarmupSessionId, FixedLinkWarmupDataPlane<K, V>>()
    private var nextIndex = 0

    override suspend fun process(
        record: ConsumerRecord<K, V>,
        processLocally: suspend () -> RecordProcessingOutcome
    ): RecordProcessingOutcome {
        val selected = synchronized(lock) {
            val eligible = active.values.filter { it.canDelegateRecord(record) }
            if (eligible.isEmpty()) emptyList() else List(eligible.size) { index ->
                eligible[(nextIndex + index).mod(eligible.size)]
            }.also { nextIndex = (nextIndex + 1).mod(eligible.size) }
        }
        val reserved = selected.firstOrNull { it.tryReserve(record) }
        return reserved?.processReserved(record, processLocally) ?: processLocally()
    }

    fun activate(dataPlane: FixedLinkWarmupDataPlane<K, V>) = synchronized(lock) {
        check(active.putIfAbsent(dataPlane.sessionId, dataPlane) == null) {
            "Warm-up data plane ${dataPlane.sessionId.value} is already active"
        }
    }

    fun accept(completed: WarmupRecordCompleted): WarmupResultTracker.Completion =
        synchronized(lock) { active[completed.recordId.sessionId] }
            ?.accept(completed)
            ?: WarmupResultTracker.Completion.IGNORED

    fun stopDelegating(sessionId: WarmupSessionId) {
        synchronized(lock) { active[sessionId] }?.stopDelegating()
    }

    fun finish(sessionId: WarmupSessionId) {
        val completed = synchronized(lock) {
            active.remove(sessionId)
        }
        completed?.close()
    }

    override fun close() {
        val completed = synchronized(lock) { active.values.toList().also { active.clear() } }
        completed.forEach(FixedLinkWarmupDataPlane<K, V>::close)
    }
}
