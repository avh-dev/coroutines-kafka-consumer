package avh.ckc.core.warmup

import avh.ckc.core.ProcessingMode
import org.apache.kafka.common.TopicPartition
import java.nio.ByteBuffer
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.time.Duration.Companion.seconds

class WarmupControlCodecTest {
    private val recipient = WarmupInstanceId("recipient-1")
    private val donor = WarmupInstanceId("donor-1")
    private val session = WarmupSessionId("session-1")
    private val consumer = WarmupConsumerId("orders")
    private val participant = WarmupParticipantConfiguration(
        consumerId = consumer,
        topicMappings = listOf(WarmupTopicMapping("orders", "warmup-orders")),
        forwardingProfile = WarmupForwardingProfile(0.01, 0.1, 20.seconds, 30.seconds),
        processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
    )
    private val assignment = WarmupSessionAssignment(
        sessionId = session,
        donorInstanceId = donor,
        recipientInstanceId = recipient,
        partitions = listOf(WarmupPartitionAssignment(consumer, "orders", "warmup-orders", 2))
    )

    @Test
    fun `all control messages have deterministic bounded round trips`() {
        val capability = WarmupDonorCapability(
            instanceId = donor,
            available = true,
            participants = listOf(WarmupParticipantCapability(
                consumer,
                listOf(WarmupTopicCapability("orders", true, setOf(1, 3), 42))
            )),
            loadScore = 0.75
        )
        val messages = listOf<WarmupControlMessage>(
            StateSyncRequest(session, recipient),
            StateSnapshot(session, donor, recipient, 3, donor, 12, 11, setOf(donor), listOf(
                WarmupReplicatedSession(assignment, WarmupReplicatedStage.FORWARDING)
            )),
            InstanceReady(session, donor),
            CoordinatorHeartbeat(session, donor, 3),
            CoordinatorClaim(session, recipient, 3, donor, 12),
            WarmupRequest(session, recipient, listOf(participant)),
            DonorProbe(session, donor, recipient, 3, recipient, listOf(participant)),
            DonorOffer(session, donor, recipient, 3, capability),
            AssignmentProposed(session, donor, 3, assignment),
            RecipientReady(session, recipient, donor, setOf(consumer)),
            StartForwarding(session, donor, donor, 3),
            StopForwarding(session, donor, donor, 3),
            ForwardingStopped(session, donor, recipient, listOf(
                WarmupFinalOffset(consumer, TopicPartition("warmup-orders", 2), 91),
                WarmupFinalOffset(WarmupConsumerId("empty"), TopicPartition("warmup-empty", 0), null)
            )),
            DrainRecipient(session, donor, recipient, 3, listOf(
                WarmupFinalOffset(consumer, TopicPartition("warmup-orders", 2), 91)
            )),
            SessionCompleted(session, recipient, setOf(consumer)),
            SessionFailed(session, donor, recipient, "producer acknowledgement failed")
        )

        messages.forEach { message ->
            val encoded = WarmupControlCodec.encode(message)
            val decoded = WarmupControlCodec.decode(encoded)
            assertEquals(message::class, decoded::class)
            assertContentEquals(encoded, WarmupControlCodec.encode(decoded))
        }

        val decodedRequest = assertIs<WarmupRequest>(
            WarmupControlCodec.decode(WarmupControlCodec.encode(messages[5]))
        )
        assertEquals(participant, decodedRequest.participants.single())
        val decodedOffer = assertIs<DonorOffer>(
            WarmupControlCodec.decode(WarmupControlCodec.encode(messages[7]))
        )
        assertEquals(capability, decodedOffer.capability)
        val decodedStopped = assertIs<ForwardingStopped>(
            WarmupControlCodec.decode(WarmupControlCodec.encode(messages[12]))
        )
        assertEquals(91, decodedStopped.finalOffsets.first().offset)
        assertEquals(null, decodedStopped.finalOffsets.last().offset)
    }

    @Test
    fun `codec rejects unsupported versions unknown types trailing bytes and oversized input`() {
        val encoded = WarmupControlCodec.encode(StopForwarding(session, recipient, donor, 1))

        val unsupportedVersion = encoded.copyOf()
        ByteBuffer.wrap(unsupportedVersion).putShort(4, 3)
        assertFailsWith<IllegalArgumentException> { WarmupControlCodec.decode(unsupportedVersion) }

        val unknownType = encoded.copyOf().also { it[6] = 127 }
        assertFailsWith<IllegalArgumentException> { WarmupControlCodec.decode(unknownType) }
        assertFailsWith<IllegalArgumentException> { WarmupControlCodec.decode(encoded + byteArrayOf(1)) }
        assertFailsWith<IllegalArgumentException> {
            WarmupControlCodec.decode(ByteArray(1024 * 1024 + 1))
        }
    }

    @Test
    fun `codec enforces encoded string and collection limits`() {
        assertFailsWith<IllegalArgumentException> {
            WarmupControlCodec.encode(SessionFailed(session, donor, recipient, "x".repeat(4097)))
        }
        assertFailsWith<IllegalArgumentException> {
            WarmupControlCodec.encode(RecipientReady(
                session,
                recipient,
                donor,
                (0..1024).map { WarmupConsumerId("consumer-$it") }
            ))
        }
    }

    @Test
    fun `codec stops encoding as soon as the message byte limit is exceeded`() {
        val largeCapability = WarmupDonorCapability(
            instanceId = donor,
            available = true,
            participants = listOf(WarmupParticipantCapability(
                consumer,
                (0 until 300).map { topic ->
                    WarmupTopicCapability(
                        sourceTopic = "topic-$topic",
                        compatible = true,
                        ownedPartitions = (0 until 1024).toSet(),
                        observedMessages = 1
                    )
                }
            )),
            loadScore = null
        )

        val error = assertFailsWith<IllegalArgumentException> {
            WarmupControlCodec.encode(DonorOffer(session, donor, recipient, 1, largeCapability))
        }
        assertEquals("Warm-up control message exceeds 1048576 bytes", error.message)
    }
}
