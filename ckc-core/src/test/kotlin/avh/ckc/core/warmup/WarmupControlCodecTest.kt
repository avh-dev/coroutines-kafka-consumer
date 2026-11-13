package avh.ckc.core.warmup

import avh.ckc.core.metrics.RecordDropReason
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer

class WarmupControlCodecTest {
    private val recipient = WarmupInstanceId("recipient-1")
    private val donor = WarmupInstanceId("donor-1")
    private val session = WarmupSessionId("session-1")
    private val fingerprint = "config-sha256"
    private val deadline = 10_000L
    private val mapping = WarmupTopicMapping(WarmupConsumerId("orders"), "orders", "warmup-orders")
    private val assignment = WarmupPartitionAssignment(mapping, 2)

    @Test
    fun `every minimal control message has deterministic bounded round trip`() {
        val messages = listOf<WarmupControlMessage>(
            WarmupRequested(session, recipient, listOf(mapping), 2_000L, fingerprint, deadline),
            DonorOffered(
                session, donor, recipient, listOf(WarmupDonorMapping(mapping, setOf(3, 1))),
                fingerprint, deadline,
                stateSnapshot = WarmupStateSnapshot(
                    17,
                    listOf(
                        WarmupSessionSnapshot(
                            session, recipient, fingerprint, WarmupReservationId("reservation-1"), listOf(assignment),
                            setOf(donor), WarmupSessionLifecycle.READY, deadline, 17
                        )
                    )
                )
            ),
            ReservationRequested(
                session, recipient, WarmupReservationId("reservation-1"), listOf(assignment), fingerprint, deadline
            ),
            ReservationReleased(
                session, recipient, WarmupReservationId("reservation-1"), WarmupReservationReleaseKind.NORMAL,
                9_000L, fingerprint, deadline
            ),
            SessionAccepted(session, recipient, donor, listOf(assignment), fingerprint, deadline),
            DonorAccepted(session, donor, recipient, fingerprint, deadline),
            SessionRejected(session, donor, recipient, "busy", fingerprint, deadline),
            RecipientReady(session, recipient, donor, listOf(assignment), fingerprint, deadline),
            ControlRecordCompleted(
                session,
                recipient,
                donor,
                WarmupRecordCompleted(
                    WarmupRecordId(session, TopicPartition("warmup-orders", 2), 91L),
                    WarmupRemoteOutcome.Discarded(RecordDropReason.STALE_AGE)
                ),
                fingerprint,
                deadline
            ),
            WarmupCompletionRequested(session, recipient, donor, fingerprint, deadline),
            DonorClosed(session, donor, recipient, fingerprint, deadline),
            SessionEnded(
                session, recipient, WarmupSessionEndState.COLD_FALLBACK, "conflict", fingerprint, deadline
            )
        )

        messages.forEach { message ->
            val encoded = WarmupControlCodec.encode(message)
            val decoded = WarmupControlCodec.decode(encoded)
            assertEquals(message, decoded)
            assertArrayEquals(encoded, WarmupControlCodec.encode(decoded))
        }
    }

    @Test
    fun `codec rejects unsupported versions unknown types trailing bytes and oversized input`() {
        val encoded = WarmupControlCodec.encode(
            DonorAccepted(session, donor, recipient, fingerprint, deadline)
        )
        val unsupportedVersion = encoded.copyOf().also { ByteBuffer.wrap(it).putShort(4, 1) }
        val unknownType = encoded.copyOf().also { it[6] = 127 }

        assertThrows(IllegalArgumentException::class.java) { WarmupControlCodec.decode(unsupportedVersion) }
        assertThrows(IllegalArgumentException::class.java) { WarmupControlCodec.decode(unknownType) }
        assertThrows(IllegalArgumentException::class.java) {
            WarmupControlCodec.decode(encoded + byteArrayOf(1))
        }
        assertThrows(IllegalArgumentException::class.java) {
            WarmupControlCodec.decode(ByteArray(1024 * 1024 + 1))
        }
    }

    @Test
    fun `codec rejects oversized strings and collections while encoding`() {
        assertThrows(IllegalArgumentException::class.java) {
            WarmupControlCodec.encode(
                SessionRejected(session, donor, recipient, "x".repeat(4_097), fingerprint, deadline)
            )
        }
        assertThrows(IllegalArgumentException::class.java) {
            WarmupControlCodec.encode(
                WarmupRequested(
                    session,
                    recipient,
                    List(4_097) { WarmupTopicMapping(WarmupConsumerId("c-$it"), "s-$it", "w-$it") },
                    2_000L,
                    fingerprint,
                    deadline
                )
            )
        }
    }
}
