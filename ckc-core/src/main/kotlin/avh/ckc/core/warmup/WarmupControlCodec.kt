package avh.ckc.core.warmup

import avh.ckc.core.ProcessingMode
import org.apache.kafka.common.TopicPartition
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.nio.charset.StandardCharsets
import kotlin.time.Duration.Companion.milliseconds

internal object WarmupControlCodec {
    private const val MAGIC = 0x434B4357
    private const val MAX_MESSAGE_BYTES = 1024 * 1024
    private const val MAX_STRING_BYTES = 4096
    private const val MAX_COLLECTION_SIZE = 1024

    fun encode(message: WarmupControlMessage): ByteArray {
        require(message.protocolVersion == WARMUP_CONTROL_PROTOCOL_VERSION) {
            "Unsupported warm-up protocol version: ${message.protocolVersion}"
        }
        val output = SizeLimitedByteArrayOutputStream(MAX_MESSAGE_BYTES)
        DataOutputStream(output).use { data ->
            data.writeInt(MAGIC)
            data.writeShort(message.protocolVersion)
            data.writeByte(message.typeId())
            data.writeString(message.sessionId.value)
            data.writeString(message.senderInstanceId.value)
            data.writeNullableString(message.targetInstanceId?.value)
            when (message) {
                is StateSyncRequest -> Unit
                is StateSnapshot -> {
                    data.writeLong(message.generation)
                    data.writeString(message.activeCoordinator.value)
                    data.writeLong(message.coveredOffset)
                    data.writeLong(message.lastHeartbeatOffset)
                    data.writeCollection(message.readyInstances.sortedBy { it.value }) { writeString(it.value) }
                    data.writeCollection(message.sessions) { writeReplicatedSession(it) }
                }
                is InstanceReady -> Unit
                is CoordinatorHeartbeat -> data.writeLong(message.generation)
                is CoordinatorClaim -> {
                    data.writeLong(message.expectedGeneration)
                    data.writeNullableString(message.expectedCoordinator?.value)
                    data.writeLong(message.observedHeartbeatOffset)
                }
                is WarmupRequest -> data.writeCollection(message.participants) { writeParticipantConfiguration(it) }
                is DonorProbe -> {
                    data.writeLong(message.generation)
                    data.writeString(message.recipientInstanceId.value)
                    data.writeCollection(message.participants) { writeParticipantConfiguration(it) }
                }
                is DonorOffer -> {
                    data.writeLong(message.generation)
                    data.writeDonorCapability(message.capability)
                }
                is AssignmentProposed -> {
                    data.writeLong(message.generation)
                    data.writeAssignment(message.assignment)
                }
                is RecipientReady -> data.writeCollection(message.consumerIds.sortedBy { it.value }) { writeString(it.value) }
                is StartForwarding -> data.writeLong(message.generation)
                is StopForwarding -> data.writeLong(message.generation)
                is ForwardingStopped -> data.writeCollection(message.finalOffsets) { writeFinalOffset(it) }
                is DrainRecipient -> {
                    data.writeLong(message.generation)
                    data.writeCollection(message.finalOffsets) { writeFinalOffset(it) }
                }
                is SessionCompleted -> data.writeCollection(message.consumerIds.sortedBy { it.value }) { writeString(it.value) }
                is SessionFailed -> data.writeString(message.reason)
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
                "Unsupported warm-up protocol version: $version"
            }
            val type = data.readUnsignedByte()
            val sessionId = WarmupSessionId(data.readString())
            val sender = WarmupInstanceId(data.readString())
            val target = data.readNullableString()?.let(::WarmupInstanceId)
            when (type) {
                TYPE_SYNC_REQUEST -> StateSyncRequest(sessionId, sender, version)
                TYPE_SNAPSHOT -> StateSnapshot(
                    sessionId, sender, requireNotNull(target), data.readLong(), WarmupInstanceId(data.readString()),
                    data.readLong(), data.readLong(), data.readCollection { WarmupInstanceId(readString()) },
                    data.readCollection { readReplicatedSession() }, version
                )
                TYPE_INSTANCE_READY -> InstanceReady(sessionId, sender, version)
                TYPE_HEARTBEAT -> CoordinatorHeartbeat(sessionId, sender, data.readLong(), version)
                TYPE_CLAIM -> CoordinatorClaim(
                    sessionId, sender, data.readLong(), data.readNullableString()?.let(::WarmupInstanceId),
                    data.readLong(), version
                )
                TYPE_REQUEST -> {
                    require(target == null) { "WarmupRequest must be broadcast" }
                    WarmupRequest(sessionId, sender, data.readCollection { readParticipantConfiguration() }, version)
                }
                TYPE_PROBE -> DonorProbe(
                    sessionId, sender, requireNotNull(target), data.readLong(), WarmupInstanceId(data.readString()),
                    data.readCollection { readParticipantConfiguration() }, version
                )
                TYPE_OFFER -> DonorOffer(
                    sessionId, sender, requireNotNull(target), data.readLong(), data.readDonorCapability(), version
                )
                TYPE_ASSIGNMENT -> AssignmentProposed(
                    sessionId, sender, data.readLong(), data.readAssignment(), version
                )
                TYPE_READY -> RecipientReady(
                    sessionId, sender, requireNotNull(target) { "RecipientReady target is missing" },
                    data.readCollection { WarmupConsumerId(readString()) }, version
                )
                TYPE_START -> StartForwarding(sessionId, sender, requireNotNull(target), data.readLong(), version)
                TYPE_STOP -> StopForwarding(sessionId, sender, requireNotNull(target), data.readLong(), version)
                TYPE_FORWARDING_STOPPED -> ForwardingStopped(
                    sessionId, sender, requireNotNull(target) { "ForwardingStopped target is missing" },
                    data.readCollection { readFinalOffset() }, version
                )
                TYPE_DRAIN -> DrainRecipient(
                    sessionId, sender, requireNotNull(target), data.readLong(),
                    data.readCollection { readFinalOffset() }, version
                )
                TYPE_COMPLETED -> SessionCompleted(
                    sessionId, sender, data.readCollection { WarmupConsumerId(readString()) }, version
                )
                TYPE_FAILED -> SessionFailed(sessionId, sender, target, data.readString(), version)
                else -> throw IllegalArgumentException("Unknown warm-up control message type: $type")
            }
        }
        require(input.available() == 0) { "Warm-up control message contains trailing bytes" }
        return message
    }

    private fun WarmupControlMessage.typeId(): Int = when (this) {
        is StateSyncRequest -> TYPE_SYNC_REQUEST
        is StateSnapshot -> TYPE_SNAPSHOT
        is InstanceReady -> TYPE_INSTANCE_READY
        is CoordinatorHeartbeat -> TYPE_HEARTBEAT
        is CoordinatorClaim -> TYPE_CLAIM
        is WarmupRequest -> TYPE_REQUEST
        is DonorProbe -> TYPE_PROBE
        is DonorOffer -> TYPE_OFFER
        is AssignmentProposed -> TYPE_ASSIGNMENT
        is RecipientReady -> TYPE_READY
        is StartForwarding -> TYPE_START
        is StopForwarding -> TYPE_STOP
        is ForwardingStopped -> TYPE_FORWARDING_STOPPED
        is DrainRecipient -> TYPE_DRAIN
        is SessionCompleted -> TYPE_COMPLETED
        is SessionFailed -> TYPE_FAILED
    }

    private fun DataOutputStream.writeParticipantConfiguration(value: WarmupParticipantConfiguration) {
        writeString(value.consumerId.value)
        writeCollection(value.topicMappings) {
            writeString(it.sourceTopic)
            writeString(it.warmupTopic)
        }
        writeDouble(value.forwardingProfile.initialFraction)
        writeDouble(value.forwardingProfile.targetFraction)
        val rampMillis = value.forwardingProfile.rampUpDuration.inWholeMilliseconds
        require(value.forwardingProfile.rampUpDuration == rampMillis.milliseconds) {
            "Warm-up ramp-up duration must have millisecond precision"
        }
        val holdMillis = value.forwardingProfile.holdDuration?.inWholeMilliseconds
        require(value.forwardingProfile.holdDuration == null ||
            value.forwardingProfile.holdDuration == holdMillis!!.milliseconds) {
            "Warm-up hold duration must have millisecond precision"
        }
        writeLong(rampMillis)
        writeLong(holdMillis ?: -1L)
        writeByte(PROCESSING_MODE_UNORDERED)
    }

    private fun DataInputStream.readParticipantConfiguration(): WarmupParticipantConfiguration {
        val consumerId = WarmupConsumerId(readString())
        val mappings = readCollection { WarmupTopicMapping(readString(), readString()) }
        val initialFraction = readDouble()
        val targetFraction = readDouble()
        val rampMillis = readLong().also { require(it >= 0) { "Negative ramp-up duration" } }
        val holdMillis = readLong().also { require(it >= -1) { "Invalid hold duration" } }
        require(readUnsignedByte() == PROCESSING_MODE_UNORDERED) { "Unsupported warm-up processing mode" }
        return WarmupParticipantConfiguration(
            consumerId = consumerId,
            topicMappings = mappings,
            forwardingProfile = WarmupForwardingProfile(
                initialFraction = initialFraction,
                targetFraction = targetFraction,
                rampUpDuration = rampMillis.milliseconds,
                holdDuration = holdMillis.takeIf { it >= 0 }?.milliseconds
            ),
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
        )
    }

    private fun DataOutputStream.writeDonorCapability(value: WarmupDonorCapability) {
        writeString(value.instanceId.value)
        writeBoolean(value.available)
        writeCollection(value.participants) { participant ->
            writeString(participant.consumerId.value)
            writeCollection(participant.topics) { topic ->
                writeString(topic.sourceTopic)
                writeBoolean(topic.compatible)
                writeCollection(topic.ownedPartitions.sorted()) { writeInt(it) }
                writeLong(topic.observedMessages)
            }
        }
        writeBoolean(value.loadScore != null)
        value.loadScore?.let(::writeDouble)
    }

    private fun DataInputStream.readDonorCapability(): WarmupDonorCapability = WarmupDonorCapability(
        instanceId = WarmupInstanceId(readString()),
        available = readBoolean(),
        participants = readCollection {
            WarmupParticipantCapability(
                WarmupConsumerId(readString()),
                readCollection {
                    WarmupTopicCapability(
                        sourceTopic = readString(),
                        compatible = readBoolean(),
                        ownedPartitions = readCollection { readInt() },
                        observedMessages = readLong()
                    )
                }
            )
        },
        loadScore = if (readBoolean()) readDouble() else null
    )

    private fun DataOutputStream.writeAssignment(value: WarmupSessionAssignment) {
        writeString(value.sessionId.value)
        writeString(value.donorInstanceId.value)
        writeString(value.recipientInstanceId.value)
        writeCollection(value.partitions) {
            writeString(it.consumerId.value)
            writeString(it.sourceTopic)
            writeString(it.warmupTopic)
            writeInt(it.warmupPartition)
        }
    }

    private fun DataInputStream.readAssignment(): WarmupSessionAssignment = WarmupSessionAssignment(
        sessionId = WarmupSessionId(readString()),
        donorInstanceId = WarmupInstanceId(readString()),
        recipientInstanceId = WarmupInstanceId(readString()),
        partitions = readCollection {
            WarmupPartitionAssignment(
                consumerId = WarmupConsumerId(readString()),
                sourceTopic = readString(),
                warmupTopic = readString(),
                warmupPartition = readInt()
            )
        }
    )

    private fun DataOutputStream.writeReplicatedSession(value: WarmupReplicatedSession) {
        writeAssignment(value.assignment)
        writeByte(value.stage.ordinal)
    }

    private fun DataInputStream.readReplicatedSession(): WarmupReplicatedSession {
        val assignment = readAssignment()
        val stageOrdinal = readUnsignedByte()
        val stage = WarmupReplicatedStage.entries.getOrNull(stageOrdinal)
            ?: throw IllegalArgumentException("Unknown replicated session stage: $stageOrdinal")
        return WarmupReplicatedSession(assignment, stage)
    }

    private fun DataOutputStream.writeFinalOffset(value: WarmupFinalOffset) {
        writeString(value.consumerId.value)
        writeString(value.topicPartition.topic())
        writeInt(value.topicPartition.partition())
        writeBoolean(value.offset != null)
        value.offset?.let(::writeLong)
    }

    private fun DataInputStream.readFinalOffset(): WarmupFinalOffset = WarmupFinalOffset(
        consumerId = WarmupConsumerId(readString()),
        topicPartition = TopicPartition(readString(), readInt()),
        offset = if (readBoolean()) readLong() else null
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

    private inline fun <T> DataOutputStream.writeCollection(values: Collection<T>, write: DataOutputStream.(T) -> Unit) {
        require(values.size <= MAX_COLLECTION_SIZE) { "Collection exceeds $MAX_COLLECTION_SIZE elements" }
        writeInt(values.size)
        values.forEach { write(it) }
    }

    private inline fun <T> DataInputStream.readCollection(decodeElement: DataInputStream.() -> T): List<T> {
        val size = readInt()
        require(size in 0..MAX_COLLECTION_SIZE) { "Invalid collection size: $size" }
        return List(size) { decodeElement() }
    }

    private const val PROCESSING_MODE_UNORDERED = 1
    private const val TYPE_SYNC_REQUEST = 1
    private const val TYPE_SNAPSHOT = 2
    private const val TYPE_INSTANCE_READY = 3
    private const val TYPE_HEARTBEAT = 4
    private const val TYPE_CLAIM = 5
    private const val TYPE_REQUEST = 6
    private const val TYPE_PROBE = 7
    private const val TYPE_OFFER = 8
    private const val TYPE_ASSIGNMENT = 9
    private const val TYPE_READY = 10
    private const val TYPE_START = 11
    private const val TYPE_STOP = 12
    private const val TYPE_FORWARDING_STOPPED = 13
    private const val TYPE_DRAIN = 14
    private const val TYPE_COMPLETED = 15
    private const val TYPE_FAILED = 16
}

private class SizeLimitedByteArrayOutputStream(
    private val maximumSize: Int
) : ByteArrayOutputStream() {
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
