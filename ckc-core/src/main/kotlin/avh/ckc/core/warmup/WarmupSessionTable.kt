package avh.ckc.core.warmup

import org.apache.kafka.common.TopicPartition

internal enum class WarmupTableDecisionKind {
    APPLIED,
    RESERVATION_WON,
    RESERVATION_CONFLICT,
    DUPLICATE,
    IGNORED_STALE,
    IGNORED_INVALID
}

internal data class WarmupTableDecision(
    val kind: WarmupTableDecisionKind,
    val sessionId: WarmupSessionId,
    val controlOffset: Long
)

/**
 * Pure, offset-ordered namespace state. It performs no Kafka I/O and never invokes participant code.
 * All members that apply the same control prefix therefore derive the same reservations.
 */
internal class WarmupSessionTable {
    private data class MutableSession(
        val sessionId: WarmupSessionId,
        val recipient: WarmupInstanceId,
        val configurationFingerprint: String,
        var reservationId: WarmupReservationId? = null,
        var assignments: List<WarmupPartitionAssignment> = emptyList(),
        val donors: MutableSet<WarmupInstanceId> = linkedSetOf(),
        var lifecycle: WarmupSessionLifecycle = WarmupSessionLifecycle.DISCOVERING,
        var expiresAtEpochMillis: Long,
        var lastControlOffset: Long
    )

    private val sessions = linkedMapOf<WarmupSessionId, MutableSession>()
    private val reservationOwners = linkedMapOf<TopicPartition, Pair<WarmupSessionId, WarmupReservationId>>()
    private var lastAppliedOffset = -1L
    private var authoritative = false

    @Synchronized
    fun apply(record: WarmupControlRecord): WarmupTableDecision {
        if (record.offset <= lastAppliedOffset) return decision(record, WarmupTableDecisionKind.IGNORED_STALE)
        lastAppliedOffset = record.offset
        val message = record.message
        val result = when (message) {
            is WarmupRequested -> applyRequest(record.offset, message)
            is ReservationRequested -> applyReservation(record.offset, message)
            is ReservationReleased -> applyRelease(record.offset, message)
            is SessionAccepted -> applySessionAccepted(record.offset, message)
            is DonorAccepted -> update(record.offset, message, message.targetInstanceId) {
                it.donors += message.senderInstanceId
            }
            is RecipientReady -> update(record.offset, message) {
                it.lifecycle = WarmupSessionLifecycle.READY
            }
            is WarmupCompletionRequested -> update(record.offset, message) {
                it.lifecycle = WarmupSessionLifecycle.CLOSING
            }
            is SessionEnded -> update(record.offset, message) {
                // Session termination does not itself release destinations. Only the matching
                // exact ReservationReleased can do that, so a lost/late terminal event
                // cannot free a partition that belongs to another reservation generation.
                it.lifecycle = if (it.reservationId == null) {
                    WarmupSessionLifecycle.CLOSED
                } else {
                    WarmupSessionLifecycle.CLOSING
                }
            }
            is DonorOffered, is SessionRejected, is ControlRecordCompleted, is DonorClosed ->
                WarmupTableDecisionKind.APPLIED
        }
        return decision(record, result)
    }

    @Synchronized
    fun install(snapshot: WarmupStateSnapshot, buffered: List<WarmupControlRecord>): Boolean {
        if (snapshot.boundaryOffset < lastAppliedOffset && authoritative) return false
        val observedLastOffset = lastAppliedOffset
        val subsequent = buffered.filter { it.offset > snapshot.boundaryOffset }.sortedBy { it.offset }
        if (observedLastOffset > snapshot.boundaryOffset && subsequent.lastOrNull()?.offset != observedLastOffset) {
            return false
        }
        if (subsequent.isNotEmpty()) {
            var expected = snapshot.boundaryOffset + 1
            for (record in subsequent) {
                if (record.offset != expected) return false
                expected++
            }
        }
        val installedSessions = linkedMapOf<WarmupSessionId, MutableSession>()
        val installedOwners = linkedMapOf<TopicPartition, Pair<WarmupSessionId, WarmupReservationId>>()
        snapshot.sessions.forEach { stored ->
            val session = MutableSession(
                stored.sessionId,
                stored.recipient,
                stored.configurationFingerprint,
                stored.reservationId,
                stored.assignments,
                stored.donors.toMutableSet(),
                stored.lifecycle,
                stored.expiresAtEpochMillis,
                stored.lastControlOffset
            )
            installedSessions[stored.sessionId] = session
            if (session.lifecycle != WarmupSessionLifecycle.CLOSED) {
                val reservation = session.reservationId
                if (reservation != null) session.assignments.forEach {
                    if (installedOwners.put(it.destination, session.sessionId to reservation) != null) return false
                }
            }
        }
        sessions.clear()
        sessions.putAll(installedSessions)
        reservationOwners.clear()
        reservationOwners.putAll(installedOwners)
        lastAppliedOffset = snapshot.boundaryOffset
        authoritative = true
        subsequent.forEach(::apply)
        return true
    }

    @Synchronized
    fun snapshot(): WarmupStateSnapshot? {
        if (!authoritative) return null
        return WarmupStateSnapshot(lastAppliedOffset, sessions.values.map { it.toSnapshot() })
    }

    @Synchronized
    fun markBootstrapAuthoritative(boundaryOffset: Long) {
        require(boundaryOffset >= -1)
        if (!authoritative) {
            lastAppliedOffset = maxOf(lastAppliedOffset, boundaryOffset)
            authoritative = true
        }
    }

    @Synchronized
    fun isAuthoritative(): Boolean = authoritative

    @Synchronized
    fun lastOffset(): Long = lastAppliedOffset

    @Synchronized
    fun reservedPartitions(): Set<TopicPartition> = reservationOwners.keys.toSet()

    @Synchronized
    fun reservationFor(sessionId: WarmupSessionId): WarmupSessionSnapshot? =
        sessions[sessionId]?.toSnapshot()

    private fun applyRequest(offset: Long, message: WarmupRequested): WarmupTableDecisionKind {
        val existing = sessions[message.sessionId]
        if (existing != null) {
            return if (existing.matches(message, message.senderInstanceId)) WarmupTableDecisionKind.DUPLICATE
            else WarmupTableDecisionKind.IGNORED_INVALID
        }
        sessions[message.sessionId] = MutableSession(
            message.sessionId,
            message.senderInstanceId,
            message.configurationFingerprint,
            expiresAtEpochMillis = message.expiresAtEpochMillis,
            lastControlOffset = offset
        )
        return WarmupTableDecisionKind.APPLIED
    }

    private fun applyReservation(offset: Long, message: ReservationRequested): WarmupTableDecisionKind {
        val session = sessions[message.sessionId] ?: return WarmupTableDecisionKind.IGNORED_INVALID
        if (!session.matches(message, message.senderInstanceId)) return WarmupTableDecisionKind.IGNORED_INVALID
        if (session.reservationId == message.reservationId && session.assignments == message.assignments) {
            session.lastControlOffset = offset
            return WarmupTableDecisionKind.DUPLICATE
        }
        if (session.reservationId != null) return WarmupTableDecisionKind.RESERVATION_CONFLICT
        if (message.assignments.any { reservationOwners.containsKey(it.destination) }) {
            session.lastControlOffset = offset
            return WarmupTableDecisionKind.RESERVATION_CONFLICT
        }
        session.reservationId = message.reservationId
        session.assignments = message.assignments.toList()
        session.lifecycle = WarmupSessionLifecycle.RESERVED
        session.lastControlOffset = offset
        session.expiresAtEpochMillis = message.expiresAtEpochMillis
        message.assignments.forEach { reservationOwners[it.destination] = session.sessionId to message.reservationId }
        return WarmupTableDecisionKind.RESERVATION_WON
    }

    private fun applySessionAccepted(offset: Long, message: SessionAccepted): WarmupTableDecisionKind {
        val session = sessions[message.sessionId] ?: return WarmupTableDecisionKind.IGNORED_INVALID
        if (!session.matches(message, message.senderInstanceId)) return WarmupTableDecisionKind.IGNORED_INVALID
        if (session.lifecycle == WarmupSessionLifecycle.CLOSED) return WarmupTableDecisionKind.IGNORED_STALE
        if (message.targetInstanceId in session.donors) {
            session.lastControlOffset = offset
            return WarmupTableDecisionKind.DUPLICATE
        }
        if (session.lifecycle !in setOf(
                WarmupSessionLifecycle.RESERVED,
                WarmupSessionLifecycle.NEGOTIATING
            )
        ) return WarmupTableDecisionKind.IGNORED_STALE
        session.donors += message.targetInstanceId
        session.lifecycle = WarmupSessionLifecycle.NEGOTIATING
        session.lastControlOffset = offset
        return WarmupTableDecisionKind.APPLIED
    }

    private fun applyRelease(offset: Long, message: ReservationReleased): WarmupTableDecisionKind {
        val session = sessions[message.sessionId] ?: return WarmupTableDecisionKind.IGNORED_INVALID
        if (!session.matchesGeneration(message) || session.reservationId != message.reservationId) {
            return WarmupTableDecisionKind.IGNORED_STALE
        }
        if (session.lifecycle == WarmupSessionLifecycle.CLOSED) return WarmupTableDecisionKind.DUPLICATE
        when (message.kind) {
            WarmupReservationReleaseKind.NORMAL ->
                if (message.senderInstanceId != session.recipient) return WarmupTableDecisionKind.IGNORED_INVALID
            WarmupReservationReleaseKind.EMERGENCY_TIMEOUT ->
                if (message.releasedAtEpochMillis < session.expiresAtEpochMillis + WARMUP_MAX_PAIRWISE_CLOCK_SKEW_MILLIS) {
                    return WarmupTableDecisionKind.IGNORED_INVALID
                }
        }
        close(session, WarmupSessionLifecycle.CLOSED, offset)
        return WarmupTableDecisionKind.APPLIED
    }

    private inline fun update(
        offset: Long,
        message: WarmupControlMessage,
        recipient: WarmupInstanceId = message.senderInstanceId,
        action: (MutableSession) -> Unit
    ): WarmupTableDecisionKind {
        val session = sessions[message.sessionId] ?: return WarmupTableDecisionKind.IGNORED_INVALID
        if (!session.matches(message, recipient)) return WarmupTableDecisionKind.IGNORED_INVALID
        if (session.lifecycle == WarmupSessionLifecycle.CLOSED) return WarmupTableDecisionKind.IGNORED_STALE
        action(session)
        session.lastControlOffset = offset
        return WarmupTableDecisionKind.APPLIED
    }

    private fun close(session: MutableSession, lifecycle: WarmupSessionLifecycle, offset: Long) {
        val reservation = session.reservationId
        if (reservation != null) session.assignments.forEach { destination ->
            reservationOwners.remove(destination.destination, session.sessionId to reservation)
        }
        session.lifecycle = lifecycle
        session.lastControlOffset = offset
    }

    private fun MutableSession.toSnapshot() = WarmupSessionSnapshot(
        sessionId, recipient, configurationFingerprint, reservationId, assignments.toList(), donors.toSet(), lifecycle,
        expiresAtEpochMillis, lastControlOffset
    )

    private fun MutableSession.matches(message: WarmupControlMessage, expectedRecipient: WarmupInstanceId): Boolean =
        recipient == expectedRecipient && matchesGeneration(message)

    private fun MutableSession.matchesGeneration(message: WarmupControlMessage): Boolean =
        configurationFingerprint == message.configurationFingerprint && expiresAtEpochMillis == message.expiresAtEpochMillis

    private fun decision(record: WarmupControlRecord, kind: WarmupTableDecisionKind) =
        WarmupTableDecision(kind, record.message.sessionId, record.offset)
}
