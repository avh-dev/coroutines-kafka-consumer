package avh.ckc.core.warmup

import avh.ckc.core.metrics.RecordDropReason
import org.apache.kafka.common.TopicPartition
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.nio.charset.StandardCharsets

/** Bounded binary codec for the incompatible, namespace-scoped GLOBAL-5 control protocol. */
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
            data.writeString(message.namespace.value)
            data.writeString(message.sessionId.value)
            data.writeString(message.senderInstanceId.value)
            data.writeNullableString(message.targetInstanceId?.value)
            data.writeString(message.configurationFingerprint)
            data.writeLong(message.expiresAtEpochMillis)
            when (message) {
                is WarmupRequested -> {
                    data.writeLong(message.offerDeadlineEpochMillis)
                    data.writeInt(message.admissionBudgetPerSecond)
                    data.writeCollection(message.mappings) { writeMapping(it) }
                }
                is DonorOffered -> data.writeCollection(message.mappings) { writeDonorMapping(it) }
                is SessionAccepted -> {
                    data.writeInt(message.admissionBudgetPerSecond)
                    data.writeCollection(message.mappings) { writeDonorMapping(it) }
                }
                is DonorAccepted -> {
                    data.writeInt(message.admissionBudgetPerSecond)
                    data.writeCollection(message.mappings) { writeDonorMapping(it) }
                }
                is SessionRejected -> data.writeString(message.reasonCode)
                is RecipientReady -> data.writeCollection(message.assignments) { writeAssignment(it) }
                is ControlRecordCompleted -> data.writeCompleted(message.completed)
                is WarmupCompletionRequested,
                is DonorClosed -> Unit
                is SessionEnded -> {
                    data.writeByte(message.state.ordinal)
                    data.writeNullableString(message.reasonCode)
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
            val namespace = WarmupControlNamespace(data.readString())
            val sessionId = WarmupSessionId(data.readString())
            val sender = WarmupInstanceId(data.readString())
            val target = data.readNullableString()?.let(::WarmupInstanceId)
            val fingerprint = data.readString()
            val deadline = data.readLong()
            when (type) {
                TYPE_REQUESTED -> {
                    val offerDeadline = data.readLong()
                    val admissionBudgetPerSecond = data.readInt()
                    val mappings = data.readCollection { readMapping() }
                    WarmupRequested(
                        namespace, sessionId, sender, mappings, offerDeadline, admissionBudgetPerSecond,
                        fingerprint, deadline, version
                    )
                }
                else -> decodeNonRequest(
                    type, namespace, sessionId, sender, target, fingerprint, deadline, version, data
                )
            }.also { decoded ->
                val broadcast = decoded is WarmupRequested || decoded is SessionEnded
                require((decoded.targetInstanceId == null) == broadcast) {
                    "${decoded::class.simpleName} has an invalid target"
                }
            }
        }
        require(input.available() == 0) { "Warm-up control message contains trailing bytes" }
        return message
    }

    private fun decodeNonRequest(
        type: Int,
        namespace: WarmupControlNamespace,
        sessionId: WarmupSessionId,
        sender: WarmupInstanceId,
        target: WarmupInstanceId?,
        fingerprint: String,
        deadline: Long,
        version: Int,
        data: DataInputStream
    ): WarmupControlMessage {
        val requiredTarget = { messageType: String -> requireNotNull(target) { "$messageType target is missing" } }
        return when (type) {
            TYPE_DONOR_OFFERED -> DonorOffered(
                namespace, sessionId, sender, requiredTarget("DonorOffered"),
                data.readCollection { readDonorMapping() }, fingerprint, deadline, version
            )
            TYPE_SESSION_ACCEPTED -> {
                val admissionBudgetPerSecond = data.readInt()
                val mappings = data.readCollection { readDonorMapping() }
                SessionAccepted(
                    namespace, sessionId, sender, requiredTarget("SessionAccepted"), mappings,
                    admissionBudgetPerSecond, fingerprint, deadline, version
                )
            }
            TYPE_DONOR_ACCEPTED -> {
                val admissionBudgetPerSecond = data.readInt()
                val mappings = data.readCollection { readDonorMapping() }
                DonorAccepted(
                    namespace, sessionId, sender, requiredTarget("DonorAccepted"), mappings,
                    admissionBudgetPerSecond, fingerprint, deadline, version
                )
            }
            TYPE_SESSION_REJECTED -> SessionRejected(
                namespace, sessionId, sender, requiredTarget("SessionRejected"), data.readString(),
                fingerprint, deadline, version
            )
            TYPE_RECIPIENT_READY -> RecipientReady(
                namespace, sessionId, sender, requiredTarget("RecipientReady"),
                data.readCollection { readAssignment() }, fingerprint, deadline, version
            )
            TYPE_RECORD_COMPLETED -> ControlRecordCompleted(
                namespace, sessionId, sender, requiredTarget("ControlRecordCompleted"),
                data.readCompleted(sessionId), fingerprint, deadline, version
            )
            TYPE_COMPLETION_REQUESTED -> WarmupCompletionRequested(
                namespace, sessionId, sender, requiredTarget("WarmupCompletionRequested"),
                fingerprint, deadline, version
            )
            TYPE_DONOR_CLOSED -> DonorClosed(
                namespace, sessionId, sender, requiredTarget("DonorClosed"), fingerprint, deadline, version
            )
            TYPE_SESSION_ENDED -> SessionEnded(
                namespace, sessionId, sender,
                WarmupSessionEndState.entries.getOrNull(data.readUnsignedByte())
                    ?: throw IllegalArgumentException("Unknown warm-up session end state"),
                data.readNullableString(), fingerprint, deadline, version
            )
            else -> throw IllegalArgumentException("Unknown warm-up control message type: $type")
        }
    }

    private fun WarmupControlMessage.typeId(): Int = when (this) {
        is WarmupRequested -> TYPE_REQUESTED
        is DonorOffered -> TYPE_DONOR_OFFERED
        is SessionAccepted -> TYPE_SESSION_ACCEPTED
        is DonorAccepted -> TYPE_DONOR_ACCEPTED
        is SessionRejected -> TYPE_SESSION_REJECTED
        is RecipientReady -> TYPE_RECIPIENT_READY
        is ControlRecordCompleted -> TYPE_RECORD_COMPLETED
        is WarmupCompletionRequested -> TYPE_COMPLETION_REQUESTED
        is DonorClosed -> TYPE_DONOR_CLOSED
        is SessionEnded -> TYPE_SESSION_ENDED
    }

    private fun DataOutputStream.writeMapping(mapping: WarmupTopicMapping) {
        writeString(mapping.participantId.value)
        writeString(mapping.sourceTopic)
        writeString(mapping.warmupTopic)
        writeString(mapping.mappingFingerprint)
        writeInt(mapping.quota.quotaPerInterval)
        writeInt(mapping.quota.initialQuotaPerInterval)
        writeLong(mapping.quota.quotaIntervalMillis)
        writeLong(mapping.quota.quotaRampDurationMillis)
    }

    private fun DataInputStream.readMapping() = WarmupTopicMapping(
        WarmupParticipantId(readString()),
        readString(),
        readString(),
        readString(),
        WarmupMappingQuota(readInt(), readInt(), readLong(), readLong())
    )

    private fun DataOutputStream.writeDonorMapping(mapping: WarmupDonorMapping) {
        writeMapping(mapping.mapping)
        writeCollection(mapping.sourcePartitions.sortedBy { it.partition }) {
            writeInt(it.partition)
            writeLong(it.ownershipGeneration)
        }
    }

    private fun DataInputStream.readDonorMapping() = WarmupDonorMapping(
        readMapping(),
        readCollection { WarmupSourcePartition(readInt(), readLong()) }.toSet()
    )

    private fun DataOutputStream.writeAssignment(assignment: WarmupPartitionAssignment) {
        writeMapping(assignment.mapping)
        writeInt(assignment.warmupPartition)
    }

    private fun DataInputStream.readAssignment() = WarmupPartitionAssignment(readMapping(), readInt())

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
        val id = WarmupRecordId(sessionId, TopicPartition(readString(), readInt()), readLong())
        val outcome = when (readUnsignedByte()) {
            OUTCOME_SUCCEEDED -> WarmupRemoteOutcome.Succeeded
            OUTCOME_TERMINAL_FAILURE -> WarmupRemoteOutcome.TerminalFailure
            OUTCOME_DISCARDED -> WarmupRemoteOutcome.Discarded(
                recordDropReason(readUnsignedByte())
            )
            else -> throw IllegalArgumentException("Unknown warm-up remote outcome")
        }
        return WarmupRecordCompleted(id, outcome)
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
        return ByteArray(length).also(::readFully).toString(StandardCharsets.UTF_8)
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
    private const val OUTCOME_SUCCEEDED = 1
    private const val OUTCOME_TERMINAL_FAILURE = 2
    private const val OUTCOME_DISCARDED = 3
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
