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
        val output = ByteArrayOutputStream()
        DataOutputStream(output).use { data ->
            data.writeInt(MAGIC)
            data.writeShort(message.protocolVersion)
            data.writeByte(message.typeId())
            data.writeString(message.sessionId.value)
            data.writeString(message.senderInstanceId.value)
            data.writeNullableString(message.targetInstanceId?.value)
            when (message) {
                is WarmupRequest -> data.writeCollection(message.participants) { writeParticipantConfiguration(it) }
                is DonorOffer -> data.writeDonorCapability(message.capability)
                is DonorSelected -> Unit
                is WarmupAssigned -> data.writeAssignment(message.assignment)
                is RecipientReady -> data.writeCollection(message.consumerIds.sortedBy { it.value }) { writeString(it.value) }
                is WarmupFinished -> Unit
                is ForwardingStopped -> data.writeCollection(message.finalOffsets) { writeFinalOffset(it) }
                is SessionCompleted -> data.writeCollection(message.consumerIds.sortedBy { it.value }) { writeString(it.value) }
                is SessionFailed -> data.writeString(message.reason)
            }
        }
        return output.toByteArray().also {
            require(it.size <= MAX_MESSAGE_BYTES) { "Warm-up control message exceeds $MAX_MESSAGE_BYTES bytes" }
        }
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
                TYPE_REQUEST -> {
                    require(target == null) { "WarmupRequest must be broadcast" }
                    WarmupRequest(sessionId, sender, data.readCollection { readParticipantConfiguration() }, version)
                }
                TYPE_OFFER -> DonorOffer(
                    sessionId, sender, requireNotNull(target) { "DonorOffer target is missing" },
                    data.readDonorCapability(), version
                )
                TYPE_SELECTED -> DonorSelected(
                    sessionId, sender, requireNotNull(target) { "DonorSelected target is missing" }, version
                )
                TYPE_ASSIGNED -> WarmupAssigned(
                    sessionId, sender, requireNotNull(target) { "WarmupAssigned target is missing" },
                    data.readAssignment(), version
                )
                TYPE_READY -> RecipientReady(
                    sessionId, sender, requireNotNull(target) { "RecipientReady target is missing" },
                    data.readCollection { WarmupConsumerId(readString()) }, version
                )
                TYPE_FINISHED -> WarmupFinished(
                    sessionId, sender, requireNotNull(target) { "WarmupFinished target is missing" }, version
                )
                TYPE_FORWARDING_STOPPED -> ForwardingStopped(
                    sessionId, sender, requireNotNull(target) { "ForwardingStopped target is missing" },
                    data.readCollection { readFinalOffset() }, version
                )
                TYPE_COMPLETED -> SessionCompleted(
                    sessionId, sender, requireNotNull(target) { "SessionCompleted target is missing" },
                    data.readCollection { WarmupConsumerId(readString()) }, version
                )
                TYPE_FAILED -> SessionFailed(sessionId, sender, target, data.readString(), version)
                else -> throw IllegalArgumentException("Unknown warm-up control message type: $type")
            }
        }
        require(input.available() == 0) { "Warm-up control message contains trailing bytes" }
        return message
    }

    private fun WarmupControlMessage.typeId(): Int = when (this) {
        is WarmupRequest -> TYPE_REQUEST
        is DonorOffer -> TYPE_OFFER
        is DonorSelected -> TYPE_SELECTED
        is WarmupAssigned -> TYPE_ASSIGNED
        is RecipientReady -> TYPE_READY
        is WarmupFinished -> TYPE_FINISHED
        is ForwardingStopped -> TYPE_FORWARDING_STOPPED
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
    private const val TYPE_REQUEST = 1
    private const val TYPE_OFFER = 2
    private const val TYPE_SELECTED = 3
    private const val TYPE_ASSIGNED = 4
    private const val TYPE_READY = 5
    private const val TYPE_FINISHED = 6
    private const val TYPE_FORWARDING_STOPPED = 7
    private const val TYPE_COMPLETED = 8
    private const val TYPE_FAILED = 9
}
