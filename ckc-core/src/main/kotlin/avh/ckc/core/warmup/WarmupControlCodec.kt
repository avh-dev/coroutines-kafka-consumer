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
                is DonorOffered -> data.writeCollection(message.mappings) { donor ->
                    writeTopicMapping(donor.mapping)
                    writeCollection(donor.sourcePartitions.sorted()) { writeInt(it) }
                }
                is SessionAccepted -> data.writeCollection(message.assignments) { writeAssignment(it) }
                is DonorAccepted -> Unit
                is SessionRejected -> data.writeString(message.reason)
                is RecipientReady -> data.writeCollection(message.assignments) { writeAssignment(it) }
                is ControlRecordCompleted -> data.writeCompleted(message.completed)
                is ForwardingStopped -> {
                    data.writeCollection(message.finalOffsets) { writeFinalOffset(it) }
                    data.writeString(message.reason)
                }
                is RecipientDrained -> data.writeCollection(message.finalOffsets) { writeFinalOffset(it) }
                is SessionEnded -> {
                    data.writeByte(message.state.ordinal)
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
                TYPE_DONOR_OFFERED -> DonorOffered(
                    sessionId, sender, requireTarget(target, "DonorOffered"),
                    data.readCollection {
                        WarmupDonorMapping(readTopicMapping(), readCollection { readInt() }.toSet())
                    }, fingerprint, deadline, version
                )
                TYPE_SESSION_ACCEPTED -> SessionAccepted(
                    sessionId, sender, requireTarget(target, "SessionAccepted"),
                    data.readCollection { readAssignment() }, fingerprint, deadline, version
                )
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
                TYPE_FORWARDING_STOPPED -> ForwardingStopped(
                    sessionId, sender, requireTarget(target, "ForwardingStopped"),
                    data.readCollection { readFinalOffset() }, data.readString(), fingerprint, deadline, version
                )
                TYPE_RECIPIENT_DRAINED -> RecipientDrained(
                    sessionId, sender, requireTarget(target, "RecipientDrained"),
                    data.readCollection { readFinalOffset() }, fingerprint, deadline, version
                )
                TYPE_SESSION_ENDED -> SessionEnded(
                    sessionId, sender,
                    WarmupSessionEndState.entries.getOrNull(data.readUnsignedByte())
                        ?: throw IllegalArgumentException("Unknown warm-up session end state"),
                    data.readNullableString(), fingerprint, deadline, version
                )
                else -> throw IllegalArgumentException("Unknown warm-up control message type: $type")
            }.also {
                if (it is WarmupRequested || it is SessionEnded) {
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
        is SessionAccepted -> TYPE_SESSION_ACCEPTED
        is DonorAccepted -> TYPE_DONOR_ACCEPTED
        is SessionRejected -> TYPE_SESSION_REJECTED
        is RecipientReady -> TYPE_RECIPIENT_READY
        is ControlRecordCompleted -> TYPE_RECORD_COMPLETED
        is ForwardingStopped -> TYPE_FORWARDING_STOPPED
        is RecipientDrained -> TYPE_RECIPIENT_DRAINED
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

    private fun DataOutputStream.writeCompleted(completed: WarmupRecordCompleted) {
        writeString(completed.recordId.topicPartition.topic())
        writeInt(completed.recordId.topicPartition.partition())
        writeLong(completed.recordId.offset)
        when (val outcome = completed.outcome) {
            WarmupRemoteOutcome.Succeeded -> writeByte(OUTCOME_SUCCEEDED)
            WarmupRemoteOutcome.TerminalFailure -> writeByte(OUTCOME_TERMINAL_FAILURE)
            is WarmupRemoteOutcome.Discarded -> {
                writeByte(OUTCOME_DISCARDED)
                writeByte(outcome.reason.ordinal)
            }
        }
    }

    private fun DataInputStream.readCompleted(sessionId: WarmupSessionId): WarmupRecordCompleted {
        val recordId = WarmupRecordId(sessionId, TopicPartition(readString(), readInt()), readLong())
        val outcome = when (val type = readUnsignedByte()) {
            OUTCOME_SUCCEEDED -> WarmupRemoteOutcome.Succeeded
            OUTCOME_TERMINAL_FAILURE -> WarmupRemoteOutcome.TerminalFailure
            OUTCOME_DISCARDED -> WarmupRemoteOutcome.Discarded(
                RecordDropReason.entries.getOrNull(readUnsignedByte())
                    ?: throw IllegalArgumentException("Unknown warm-up discard reason")
            )
            else -> throw IllegalArgumentException("Unknown warm-up remote outcome: $type")
        }
        return WarmupRecordCompleted(recordId, outcome)
    }

    private fun DataOutputStream.writeFinalOffset(offset: WarmupFinalOffset) {
        writeString(offset.topicPartition.topic())
        writeInt(offset.topicPartition.partition())
        writeBoolean(offset.offset != null)
        offset.offset?.let(::writeLong)
    }

    private fun DataInputStream.readFinalOffset() = WarmupFinalOffset(
        TopicPartition(readString(), readInt()),
        if (readBoolean()) readLong() else null
    )

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

    private const val TYPE_REQUESTED = 1
    private const val TYPE_DONOR_OFFERED = 2
    private const val TYPE_SESSION_ACCEPTED = 3
    private const val TYPE_DONOR_ACCEPTED = 4
    private const val TYPE_SESSION_REJECTED = 5
    private const val TYPE_RECIPIENT_READY = 6
    private const val TYPE_RECORD_COMPLETED = 7
    private const val TYPE_FORWARDING_STOPPED = 8
    private const val TYPE_RECIPIENT_DRAINED = 9
    private const val TYPE_SESSION_ENDED = 10
    private const val OUTCOME_SUCCEEDED = 1
    private const val OUTCOME_TERMINAL_FAILURE = 2
    private const val OUTCOME_DISCARDED = 3
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
