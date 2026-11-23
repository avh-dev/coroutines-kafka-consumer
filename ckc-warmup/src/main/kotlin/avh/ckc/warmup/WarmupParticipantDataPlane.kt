package avh.ckc.warmup

import avh.ckc.core.processing.AdmittedRecordInterceptor
import avh.ckc.core.processing.RecordProcessingOutcome
import avh.ckc.warmup.protocol.WarmupPartitionAssignment
import avh.ckc.warmup.protocol.WarmupRecordCompleted
import avh.ckc.warmup.protocol.WarmupSessionId
import avh.ckc.warmup.protocol.WarmupTopicMapping
import avh.ckc.warmup.protocol.normalizedAdmissionBudget
import kotlin.time.Duration
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.apache.kafka.clients.consumer.ConsumerRecord

/** Typed record plane for one participant; logical-session admission remains context-wide. */
internal class WarmupParticipantDataPlane<K, V> : AdmittedRecordInterceptor<K, V> {
    private data class Selected<K, V>(
        val session: WarmupDonorSession<K, V>,
        val destination: WarmupDestination
    )

    private val lock = Any()
    private val lanes = linkedMapOf<WarmupSessionId, WarmupParticipantDonorLane<K, V>>()
    private var nextSessionIndex = 0

    fun install(lane: WarmupParticipantDonorLane<K, V>): Boolean = synchronized(lock) {
        if (lane.sessionId in lanes) return@synchronized false
        lanes[lane.sessionId] = lane
        true
    }

    fun remove(lane: WarmupParticipantDonorLane<K, V>): Boolean = synchronized(lock) {
        if (lanes[lane.sessionId] !== lane) return@synchronized false
        lanes.remove(lane.sessionId)
        if (lanes.isEmpty()) nextSessionIndex = 0 else nextSessionIndex %= lanes.size
        true
    }

    override suspend fun process(
        record: ConsumerRecord<K, V>,
        processLocally: suspend () -> RecordProcessingOutcome
    ): RecordProcessingOutcome {
        val selected = select(record) ?: return processLocally()
        return selected.session.processDelegatedRecord(record, selected.destination, processLocally)
    }

    private fun select(record: ConsumerRecord<K, V>): Selected<K, V>? = synchronized(lock) {
        val candidates = lanes.values.toList()
        for (offset in candidates.indices) {
            val index = (nextSessionIndex + offset).mod(candidates.size)
            val selected = candidates[index].tryBeginDelegation(record) ?: continue
            nextSessionIndex = (index + 1).mod(candidates.size)
            return@synchronized Selected(selected.first, selected.second)
        }
        null
    }
}

/** One typed participant lane inside a logical donor session. */
internal class WarmupParticipantDonorLane<K, V>(
    override val sessionId: WarmupSessionId,
    private val dataPlane: WarmupParticipantDataPlane<K, V>,
    mappingSessions: Map<WarmupTopicMapping, WarmupDonorSession<K, V>>
) : WarmupDonorSessionLifecycle {
    private val mappingSessions = mappingSessions.toMap()
    private val sessionsBySource = this.mappingSessions.entries.associate { it.key.sourceTopic to it.value }
    private val sessionsByDestination = this.mappingSessions.entries.associate {
        WarmupPartitionAssignment(it.key).destination to it.value
    }
    override val admissionBudgetPerSecond: Int = normalizedAdmissionBudget(this.mappingSessions.keys)
    init {
        require(mappingSessions.isNotEmpty()) { "Participant donor lane must contain mappings" }
        require(mappingSessions.keys.map { it.sourceTopic }.distinct().size == mappingSessions.size) {
            "Participant donor source topics must be unique"
        }
        require(mappingSessions.values.all { it.sessionId == sessionId }) {
            "Every mapping session must belong to its participant lane"
        }
        check(dataPlane.install(this)) { "Participant already contains donor session ${sessionId.value}" }
    }

    fun tryBeginDelegation(record: ConsumerRecord<K, V>): Pair<WarmupDonorSession<K, V>, WarmupDestination>? {
        val session = sessionsBySource[record.topic()] ?: return null
        val destination = session.tryBeginDelegation(record) ?: return null
        return session to destination
    }

    override fun activate(): Boolean {
        val activated = mappingSessions.values.map(WarmupDonorSession<K, V>::activate)
        check(activated.all { it } || activated.none { it }) { "Participant mapping sessions activated inconsistently" }
        return activated.all { it }
    }

    override fun acceptResult(completed: WarmupRecordCompleted): WarmupResultTracker.Completion =
        sessionsByDestination[completed.recordId.topicPartition]
            ?.acceptResult(completed)
            ?: WarmupResultTracker.Completion.IGNORED

    override fun stopDelegating() = mappingSessions.values.forEach(WarmupDonorSession<K, V>::stopDelegating)

    override suspend fun stopDelegatingAndAwaitInflight(timeout: Duration) = coroutineScope {
        stopDelegating()
        mappingSessions.values.map { session ->
            async { session.stopDelegatingAndAwaitInflight(timeout) }
        }.awaitAll()
        Unit
    }

    override suspend fun awaitInflightCompletion() = coroutineScope {
        mappingSessions.values.map { session -> async { session.awaitInflightCompletion() } }.awaitAll()
        Unit
    }

    override fun fail(error: Throwable) = mappingSessions.values.forEach { it.fail(error) }

    override fun trySealForRemoval(): Boolean {
        val sealed = mappingSessions.values.map(WarmupDonorSession<K, V>::trySealForRemoval)
        check(sealed.all { it } || sealed.none { it }) { "Participant mapping sessions sealed inconsistently" }
        if (!sealed.all { it }) return false
        check(dataPlane.remove(this)) { "Sealed participant donor lane was not installed" }
        return true
    }

    override fun close() = mappingSessions.values.toList().asReversed().forEach(WarmupDonorSession<K, V>::close)
}
