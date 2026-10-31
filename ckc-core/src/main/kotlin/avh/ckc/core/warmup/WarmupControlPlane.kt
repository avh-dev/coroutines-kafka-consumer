package avh.ckc.core.warmup

import java.util.TreeMap

data class WarmupControlPlaneLimits(
    val maximumBufferedRecords: Int = 2048,
    val maximumSessions: Int = 128,
    val maximumReadyInstances: Int = 256
) {
    init {
        require(maximumBufferedRecords > 0)
        require(maximumSessions > 0)
        require(maximumReadyInstances > 0)
    }
}

internal data class WarmupControlPlaneView(
    val synchronized: Boolean,
    val generation: Long,
    val activeCoordinator: WarmupInstanceId?,
    val lastAppliedOffset: Long,
    val lastHeartbeatOffset: Long,
    val readyInstances: Set<WarmupInstanceId>,
    val sessions: Map<WarmupSessionId, WarmupReplicatedSession>
)

internal data class WarmupControlApplication(
    val accepted: Boolean,
    val synchronizedNow: Boolean = false
)

/** Deterministic state derived only from records observed in the one-partition control journal. */
internal class WarmupControlPlaneReplica(
    private val localInstanceId: WarmupInstanceId,
    private val initialLogPosition: WarmupControlLogPosition,
    private val syncRequestId: WarmupSessionId,
    private val limits: WarmupControlPlaneLimits = WarmupControlPlaneLimits()
) {
    private val buffer = TreeMap<Long, WarmupControlMessage>()
    private val readyInstances = linkedSetOf<WarmupInstanceId>()
    private val sessions = linkedMapOf<WarmupSessionId, WarmupReplicatedSession>()
    private var synchronized = false
    private var generation = 0L
    private var activeCoordinator: WarmupInstanceId? = null
    private var lastAppliedOffset = initialLogPosition.endOffset - 1
    private var lastHeartbeatOffset = -1L

    @Synchronized
    fun bootstrapEmptyJournal(): Boolean {
        if (!initialLogPosition.isProvablyEmpty || synchronized) return false
        synchronized = true
        lastAppliedOffset = -1
        return true
    }

    @Synchronized
    fun apply(record: WarmupControlRecord): WarmupControlApplication {
        if (!synchronized) {
            val snapshot = record.message as? StateSnapshot
            if (snapshot != null && snapshot.sessionId == syncRequestId &&
                snapshot.targetInstanceId == localInstanceId) {
                return installSnapshot(snapshot, record.offset)
            }
            check(buffer.size < limits.maximumBufferedRecords) {
                "Warm-up state synchronization buffer exceeded ${limits.maximumBufferedRecords} records"
            }
            buffer.putIfAbsent(record.offset, record.message)
            return WarmupControlApplication(false)
        }
        if (record.offset <= lastAppliedOffset) return WarmupControlApplication(false)
        val accepted = applyMessage(record.offset, record.message)
        lastAppliedOffset = record.offset
        return WarmupControlApplication(accepted)
    }

    @Synchronized
    fun view(): WarmupControlPlaneView = WarmupControlPlaneView(
        synchronized, generation, activeCoordinator, lastAppliedOffset, lastHeartbeatOffset,
        readyInstances.toSet(), sessions.toMap()
    )

    @Synchronized
    fun snapshot(request: StateSyncRequest): StateSnapshot? {
        val active = activeCoordinator
        if (!synchronized || active != localInstanceId || generation <= 0) return null
        return StateSnapshot(
            request.sessionId, localInstanceId, request.senderInstanceId, generation, active,
            lastAppliedOffset, lastHeartbeatOffset, readyInstances, sessions.values
        )
    }

    @Synchronized
    fun commandIsCurrent(message: GeneratedWarmupCommand): Boolean =
        synchronized && message.generation == generation && message.senderInstanceId == activeCoordinator

    private fun installSnapshot(snapshot: StateSnapshot, snapshotRecordOffset: Long): WarmupControlApplication {
        // We started at initialLogPosition.endOffset. The snapshot must cover everything before that point;
        // Kafka offsets themselves need not be contiguous.
        check(initialLogPosition.beginningOffset <= snapshot.coveredOffset + 1) {
            "Required control log range is no longer retained"
        }
        check(snapshot.coveredOffset >= initialLogPosition.endOffset - 1) {
            "State snapshot does not cover the unread control log range"
        }
        check(snapshot.readyInstances.size <= limits.maximumReadyInstances)
        check(snapshot.sessions.size <= limits.maximumSessions)
        check(snapshot.activeCoordinator in snapshot.readyInstances) {
            "Snapshot active coordinator is not election-eligible"
        }
        check(snapshot.sessions.map { it.assignment.sessionId }.toSet().size == snapshot.sessions.size) {
            "Snapshot contains duplicate session identifiers"
        }
        val destinations = snapshot.sessions.flatMap { it.assignment.partitions }
            .map { it.topicPartition }
        check(destinations.toSet().size == destinations.size) {
            "Snapshot contains overlapping warm-up reservations"
        }
        generation = snapshot.generation
        activeCoordinator = snapshot.activeCoordinator
        lastHeartbeatOffset = snapshot.lastHeartbeatOffset
        readyInstances.clear()
        readyInstances += snapshot.readyInstances
        sessions.clear()
        snapshot.sessions.forEach { session -> sessions[session.assignment.sessionId] = session }
        synchronized = true
        lastAppliedOffset = snapshot.coveredOffset
        val tail = buffer.entries.filter { it.key > snapshot.coveredOffset } +
            if (snapshotRecordOffset > snapshot.coveredOffset) listOf(
                java.util.AbstractMap.SimpleImmutableEntry(snapshotRecordOffset, snapshot as WarmupControlMessage)
            ) else emptyList()
        buffer.clear()
        tail.sortedBy { it.key }.forEach { (offset, message) ->
            if (offset > lastAppliedOffset) {
                applyMessage(offset, message)
                lastAppliedOffset = offset
            }
        }
        return WarmupControlApplication(true, synchronizedNow = true)
    }

    private fun applyMessage(offset: Long, message: WarmupControlMessage): Boolean {
        return when (message) {
            is StateSyncRequest, is StateSnapshot, is WarmupRequest, is DonorProbe, is DonorOffer,
            is RecipientReady, is ForwardingStopped -> true
            is InstanceReady -> {
                if (readyInstances.size >= limits.maximumReadyInstances &&
                    message.senderInstanceId !in readyInstances) {
                    false
                } else readyInstances.add(message.senderInstanceId)
            }
            is CoordinatorClaim -> applyClaim(offset, message)
            is CoordinatorHeartbeat -> {
                if (!commandIsCurrent(message)) false else {
                    lastHeartbeatOffset = offset
                    true
                }
            }
            is AssignmentProposed -> applyAssignment(message)
            is StartForwarding -> updateStage(message, WarmupReplicatedStage.FORWARDING)
            is StopForwarding -> updateStage(message, WarmupReplicatedStage.STOPPING)
            is DrainRecipient -> updateStage(message, WarmupReplicatedStage.DRAINING)
            is SessionCompleted -> {
                val existing = sessions[message.sessionId]
                if (existing != null && existing.assignment.recipientInstanceId == message.senderInstanceId &&
                    message.consumerIds == existing.assignment.partitions.map { it.consumerId }.toSet()) {
                    sessions.remove(message.sessionId)
                    true
                } else existing == null // idempotent completion
            }
            is SessionFailed -> {
                val existing = sessions[message.sessionId] ?: return false
                if (message.senderInstanceId != existing.assignment.donorInstanceId &&
                    message.senderInstanceId != existing.assignment.recipientInstanceId) return false
                sessions[message.sessionId] = existing.copy(stage = WarmupReplicatedStage.QUARANTINED)
                true
            }
        }
    }

    private fun applyClaim(offset: Long, claim: CoordinatorClaim): Boolean {
        if (claim.senderInstanceId !in readyInstances || claim.expectedGeneration != generation ||
            claim.expectedCoordinator != activeCoordinator || claim.observedHeartbeatOffset != lastHeartbeatOffset) return false
        generation++
        activeCoordinator = claim.senderInstanceId
        lastHeartbeatOffset = offset
        return true
    }

    private fun applyAssignment(message: AssignmentProposed): Boolean {
        if (!commandIsCurrent(message)) return false
        val existing = sessions[message.sessionId]
        if (existing != null) return existing.assignment == message.assignment
        if (sessions.size >= limits.maximumSessions) return false
        val used = sessions.values.flatMap { it.assignment.partitions }.map { it.topicPartition }.toSet()
        if (message.assignment.partitions.any { it.topicPartition in used }) return false
        sessions[message.sessionId] = WarmupReplicatedSession(message.assignment, WarmupReplicatedStage.ASSIGNED)
        return true
    }

    private fun updateStage(message: GeneratedWarmupCommand, stage: WarmupReplicatedStage): Boolean {
        if (!commandIsCurrent(message)) return false
        val existing = sessions[message.sessionId] ?: return false
        sessions[message.sessionId] = existing.copy(stage = stage)
        return true
    }
}
