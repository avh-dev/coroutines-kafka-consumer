package avh.ckc.core.warmup

import avh.ckc.core.metrics.RecordDropReason
import org.apache.kafka.common.TopicPartition
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.nio.charset.StandardCharsets

internal object WarmupControlCodec {
    private const val MAGIC = 0x434B5457
    private const val MAX_MESSAGE_BYTES = 1024 * 1024
    private const val MAX_STRING_BYTES = 4096
    private const val MAX_COLLECTION_SIZE = 4096

    fun encode(message: WarmupControlMessage): ByteArray {
        require(message.protocolVersion == WARMUP_CONTROL_PROTOCOL_VERSION) {
            "Unsupported warm-up control protocol version: ${message.protocolVersion}"
        }
        val output = SizeLimitedByteArrayOutputStream(MAX_MESSAGE_BYTES)
        DataOutputStream(output).use { data ->
            data.writeInt(MAGIC)
            data.writeShort(message.protocolVersion)
            data.writeByte(message.typeId())
            data.writeString(message.sessionId.value)
            data.writeString(message.senderInstanceId.value)
            data.writeNullableString(message.targetInstanceId?.value)
            data.writeString(message.configurationFingerprint)
            data.writeLong(message.expiresAtEpochMillis)
            when (message) {
                is WarmupRequested -> {
                    data.writeLong(message.offerDeadlineEpochMillis)
                    data.writeCollection(message.mappings) { writeTopicMapping(it) }
                }
                is DonorOffered -> {
                    data.writeCollection(message.mappings) { donor ->
                        writeTopicMapping(donor.mapping)
                        writeCollection(donor.sourcePartitions.sorted()) { writeInt(it) }
                    }
                    data.writeBoolean(message.stateSnapshot != null)
                    message.stateSnapshot?.let { data.writeSnapshot(it) }
                }
                is ReservationRequested -> {
                    data.writeString(message.reservationId.value)
                    data.writeCollection(message.assignments) { writeAssignment(it) }
                }
                is ReservationReleased -> data.writeString(message.reservationId.value)
                is SessionAccepted -> {
                    data.writeCollection(message.assignments) { writeAssignment(it) }
                }
                is DonorAccepted -> Unit
                is SessionRejected -> data.writeString(message.reason)
                is RecipientReady -> data.writeCollection(message.assignments) { writeAssignment(it) }
                is ControlRecordCompleted -> data.writeCompleted(message.completed)
                is WarmupCompletionRequested -> Unit
                is DonorClosed -> Unit
                is SessionEnded -> {
                    data.writeByte(message.state.wireId())
                    data.writeNullableString(message.reason)
                }
            }
        }
        return output.toByteArray()
    }

    fun decode(bytes: ByteArray): WarmupControlMessage {
        require(bytes.size <= MAX_MESSAGE_BYTES) { "Warm-up control message exceeds $MAX_MESSAGE_BYTES bytes" }
        val input = ByteArrayInputStream(bytes)
        val message = DataInputStream(input).use { data ->
            require(data.readInt() == MAGIC) { "Invalid warm-up control message magic" }
            val version = data.readUnsignedShort()
            require(version == WARMUP_CONTROL_PROTOCOL_VERSION) {
                "Unsupported warm-up control protocol version: $version"
            }
            val type = data.readUnsignedByte()
            val sessionId = WarmupSessionId(data.readString())
            val sender = WarmupInstanceId(data.readString())
            val target = data.readNullableString()?.let(::WarmupInstanceId)
            val fingerprint = data.readString()
            val deadline = data.readLong()
            when (type) {
                TYPE_REQUESTED -> WarmupRequested(
                    sessionId, sender,
                    offerDeadlineEpochMillis = data.readLong(),
                    mappings = data.readCollection { readTopicMapping() },
                    configurationFingerprint = fingerprint,
                    expiresAtEpochMillis = deadline,
                    protocolVersion = version
                )
                TYPE_DONOR_OFFERED -> {
                    val mappings = data.readCollection {
                        WarmupDonorMapping(readTopicMapping(), readCollection { readInt() }.toSet())
                    }
                    val snapshot = if (data.readBoolean()) data.readSnapshot() else null
                    DonorOffered(
                        sessionId, sender, requireTarget(target, "DonorOffered"),
                        mappings, fingerprint, deadline, version, snapshot
                    )
                }
                TYPE_RESERVATION_REQUESTED -> ReservationRequested(
                    sessionId, sender, WarmupReservationId(data.readString()),
                    data.readCollection { readAssignment() }, fingerprint, deadline, version
                )
                TYPE_RESERVATION_RELEASED -> ReservationReleased(
                    sessionId, sender, WarmupReservationId(data.readString()), fingerprint, deadline, version
                )
                TYPE_SESSION_ACCEPTED -> {
                    SessionAccepted(
                        sessionId, sender, requireTarget(target, "SessionAccepted"),
                        data.readCollection { readAssignment() }, fingerprint, deadline, version
                    )
                }
                TYPE_DONOR_ACCEPTED -> DonorAccepted(
                    sessionId, sender, requireTarget(target, "DonorAccepted"), fingerprint, deadline, version
                )
                TYPE_SESSION_REJECTED -> SessionRejected(
                    sessionId, sender, requireTarget(target, "SessionRejected"), data.readString(),
                    fingerprint, deadline, version
                )
                TYPE_RECIPIENT_READY -> RecipientReady(
                    sessionId, sender, requireTarget(target, "RecipientReady"),
                    data.readCollection { readAssignment() }, fingerprint, deadline, version
                )
                TYPE_RECORD_COMPLETED -> ControlRecordCompleted(
                    sessionId, sender, requireTarget(target, "ControlRecordCompleted"),
                    data.readCompleted(sessionId), fingerprint, deadline, version
                )
                TYPE_COMPLETION_REQUESTED -> WarmupCompletionRequested(
                    sessionId, sender, requireTarget(target, "WarmupCompletionRequested"), fingerprint, deadline, version
                )
                TYPE_DONOR_CLOSED -> DonorClosed(
                    sessionId, sender, requireTarget(target, "DonorClosed"), fingerprint, deadline, version
                )
                TYPE_SESSION_ENDED -> SessionEnded(
                    sessionId, sender,
                    sessionEndState(data.readUnsignedByte()),
                    data.readNullableString(), fingerprint, deadline, version
                )
                else -> throw IllegalArgumentException("Unknown warm-up control message type: $type")
            }.also {
                if (it is WarmupRequested || it is ReservationRequested || it is ReservationReleased || it is SessionEnded) {
                    require(target == null) { "${it::class.simpleName} must be broadcast" }
                }
            }
        }
        require(input.available() == 0) { "Warm-up control message contains trailing bytes" }
        return message
    }

    private fun WarmupControlMessage.typeId(): Int = when (this) {
        is WarmupRequested -> TYPE_REQUESTED
        is DonorOffered -> TYPE_DONOR_OFFERED
        is ReservationRequested -> TYPE_RESERVATION_REQUESTED
        is ReservationReleased -> TYPE_RESERVATION_RELEASED
        is SessionAccepted -> TYPE_SESSION_ACCEPTED
        is DonorAccepted -> TYPE_DONOR_ACCEPTED
        is SessionRejected -> TYPE_SESSION_REJECTED
        is RecipientReady -> TYPE_RECIPIENT_READY
        is ControlRecordCompleted -> TYPE_RECORD_COMPLETED
        is WarmupCompletionRequested -> TYPE_COMPLETION_REQUESTED
        is DonorClosed -> TYPE_DONOR_CLOSED
        is SessionEnded -> TYPE_SESSION_ENDED
    }

    private fun DataOutputStream.writeTopicMapping(mapping: WarmupTopicMapping) {
        writeString(mapping.consumerId.value)
        writeString(mapping.sourceTopic)
        writeString(mapping.warmupTopic)
    }

    private fun DataInputStream.readTopicMapping() = WarmupTopicMapping(
        WarmupConsumerId(readString()), readString(), readString()
    )

    private fun DataOutputStream.writeAssignment(assignment: WarmupPartitionAssignment) {
        writeTopicMapping(assignment.mapping)
        writeInt(assignment.warmupPartition)
    }

    private fun DataInputStream.readAssignment() = WarmupPartitionAssignment(readTopicMapping(), readInt())

    private fun DataOutputStream.writeSnapshot(snapshot: WarmupStateSnapshot) {
        writeLong(snapshot.boundaryOffset)
        writeCollection(snapshot.sessions) { session ->
            writeString(session.sessionId.value)
            writeString(session.recipient.value)
            writeString(session.configurationFingerprint)
            writeNullableString(session.reservationId?.value)
            writeCollection(session.assignments) { writeAssignment(it) }
            writeCollection(session.donors.sortedBy { it.value }) { writeString(it.value) }
            writeByte(session.lifecycle.ordinal)
            writeLong(session.expiresAtEpochMillis)
            writeLong(session.lastControlOffset)
        }
    }

    private fun DataInputStream.readSnapshot(): WarmupStateSnapshot {
        val boundary = readLong()
        val sessions = readCollection {
            val sessionId = WarmupSessionId(readString())
            val recipient = WarmupInstanceId(readString())
            val sessionFingerprint = readString()
            val reservation = readNullableString()?.let(::WarmupReservationId)
            val assignments = readCollection { readAssignment() }
            val donors = readCollection { WarmupInstanceId(readString()) }.toSet()
            val lifecycleOrdinal = readUnsignedByte()
            val lifecycle = WarmupSessionLifecycle.entries.getOrNull(lifecycleOrdinal)
                ?: throw IllegalArgumentException("Unknown warm-up lifecycle: $lifecycleOrdinal")
            WarmupSessionSnapshot(
                sessionId, recipient, sessionFingerprint, reservation, assignments, donors, lifecycle,
                readLong(), readLong()
            )
        }
        return WarmupStateSnapshot(boundary, sessions)
    }

    private fun DataOutputStream.writeCompleted(completed: WarmupRecordCompleted) {
        writeString(completed.recordId.topicPartition.topic())
        writeInt(completed.recordId.topicPartition.partition())
        writeLong(completed.recordId.offset)
        when (val outcome = completed.outcome) {
            WarmupRemoteOutcome.Succeeded -> writeByte(OUTCOME_SUCCEEDED)
            WarmupRemoteOutcome.TerminalFailure -> writeByte(OUTCOME_TERMINAL_FAILURE)
            is WarmupRemoteOutcome.Discarded -> {
                writeByte(OUTCOME_DISCARDED)
                writeByte(outcome.reason.wireId())
            }
        }
    }

    private fun DataInputStream.readCompleted(sessionId: WarmupSessionId): WarmupRecordCompleted {
        val recordId = WarmupRecordId(sessionId, TopicPartition(readString(), readInt()), readLong())
        val outcome = when (val type = readUnsignedByte()) {
            OUTCOME_SUCCEEDED -> WarmupRemoteOutcome.Succeeded
            OUTCOME_TERMINAL_FAILURE -> WarmupRemoteOutcome.TerminalFailure
            OUTCOME_DISCARDED -> WarmupRemoteOutcome.Discarded(
                recordDropReason(readUnsignedByte())
            )
            else -> throw IllegalArgumentException("Unknown warm-up remote outcome: $type")
        }
        return WarmupRecordCompleted(recordId, outcome)
    }

    private fun DataOutputStream.writeString(value: String) {
        val encoded = value.toByteArray(StandardCharsets.UTF_8)
        require(encoded.size <= MAX_STRING_BYTES) { "String exceeds $MAX_STRING_BYTES encoded bytes" }
        writeInt(encoded.size)
        write(encoded)
    }

    private fun DataInputStream.readString(): String {
        val length = readInt()
        require(length in 0..MAX_STRING_BYTES) { "Invalid encoded string length: $length" }
        val bytes = ByteArray(length)
        readFully(bytes)
        return String(bytes, StandardCharsets.UTF_8)
    }

    private fun DataOutputStream.writeNullableString(value: String?) {
        writeBoolean(value != null)
        if (value != null) writeString(value)
    }

    private fun DataInputStream.readNullableString(): String? = if (readBoolean()) readString() else null

    private inline fun <T> DataOutputStream.writeCollection(
        values: Collection<T>,
        writeElement: DataOutputStream.(T) -> Unit
    ) {
        require(values.size <= MAX_COLLECTION_SIZE) { "Collection exceeds $MAX_COLLECTION_SIZE elements" }
        writeInt(values.size)
        values.forEach { writeElement(it) }
    }

    private inline fun <T> DataInputStream.readCollection(readElement: DataInputStream.() -> T): List<T> {
        val size = readInt()
        require(size in 0..MAX_COLLECTION_SIZE) { "Invalid collection size: $size" }
        return List(size) { readElement() }
    }

    private fun requireTarget(target: WarmupInstanceId?, type: String): WarmupInstanceId =
        requireNotNull(target) { "$type target is missing" }

    private fun WarmupSessionEndState.wireId(): Int = when (this) {
        WarmupSessionEndState.COMPLETED -> SESSION_COMPLETED
        WarmupSessionEndState.FAILED -> SESSION_FAILED
        WarmupSessionEndState.COLD_FALLBACK -> SESSION_COLD_FALLBACK
    }

    private fun sessionEndState(id: Int): WarmupSessionEndState = when (id) {
        SESSION_COMPLETED -> WarmupSessionEndState.COMPLETED
        SESSION_FAILED -> WarmupSessionEndState.FAILED
        SESSION_COLD_FALLBACK -> WarmupSessionEndState.COLD_FALLBACK
        else -> throw IllegalArgumentException("Unknown warm-up session end state: $id")
    }

    private fun RecordDropReason.wireId(): Int = when (this) {
        RecordDropReason.ALREADY_PROCESSED -> DISCARD_ALREADY_PROCESSED
        RecordDropReason.QUEUE_OVERFLOW -> DISCARD_QUEUE_OVERFLOW
        RecordDropReason.REPLACED_BY_NEWER_KEY_RECORD -> DISCARD_REPLACED_BY_NEWER_KEY_RECORD
        RecordDropReason.NEW_KEY_QUEUE_FULL -> DISCARD_NEW_KEY_QUEUE_FULL
        RecordDropReason.STALE_AGE -> DISCARD_STALE_AGE
        RecordDropReason.LIFECYCLE_DISCARD -> DISCARD_LIFECYCLE
    }

    private fun recordDropReason(id: Int): RecordDropReason = when (id) {
        DISCARD_ALREADY_PROCESSED -> RecordDropReason.ALREADY_PROCESSED
        DISCARD_QUEUE_OVERFLOW -> RecordDropReason.QUEUE_OVERFLOW
        DISCARD_REPLACED_BY_NEWER_KEY_RECORD -> RecordDropReason.REPLACED_BY_NEWER_KEY_RECORD
        DISCARD_NEW_KEY_QUEUE_FULL -> RecordDropReason.NEW_KEY_QUEUE_FULL
        DISCARD_STALE_AGE -> RecordDropReason.STALE_AGE
        DISCARD_LIFECYCLE -> RecordDropReason.LIFECYCLE_DISCARD
        else -> throw IllegalArgumentException("Unknown warm-up discard reason: $id")
    }

    private const val TYPE_REQUESTED = 1
    private const val TYPE_DONOR_OFFERED = 2
    private const val TYPE_SESSION_ACCEPTED = 3
    private const val TYPE_DONOR_ACCEPTED = 4
    private const val TYPE_SESSION_REJECTED = 5
    private const val TYPE_RECIPIENT_READY = 6
    private const val TYPE_RECORD_COMPLETED = 7
    private const val TYPE_COMPLETION_REQUESTED = 8
    private const val TYPE_DONOR_CLOSED = 9
    private const val TYPE_SESSION_ENDED = 10
    private const val TYPE_RESERVATION_REQUESTED = 11
    private const val TYPE_RESERVATION_RELEASED = 12
    private const val OUTCOME_SUCCEEDED = 1
    private const val OUTCOME_TERMINAL_FAILURE = 2
    private const val OUTCOME_DISCARDED = 3
    private const val SESSION_COMPLETED = 1
    private const val SESSION_FAILED = 2
    private const val SESSION_COLD_FALLBACK = 3
    private const val DISCARD_ALREADY_PROCESSED = 1
    private const val DISCARD_QUEUE_OVERFLOW = 2
    private const val DISCARD_REPLACED_BY_NEWER_KEY_RECORD = 3
    private const val DISCARD_NEW_KEY_QUEUE_FULL = 4
    private const val DISCARD_STALE_AGE = 5
    private const val DISCARD_LIFECYCLE = 6
}

private class SizeLimitedByteArrayOutputStream(private val maximumSize: Int) : ByteArrayOutputStream() {
    override fun write(value: Int) {
        requireCapacity(1)
        super.write(value)
    }

    override fun write(bytes: ByteArray, offset: Int, length: Int) {
        requireCapacity(length)
        super.write(bytes, offset, length)
    }

    private fun requireCapacity(additionalBytes: Int) {
        require(additionalBytes <= maximumSize - count) {
            "Warm-up control message exceeds $maximumSize bytes"
        }
    }
}
