package avh.ckc.core.warmup

import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class WarmupSessionTableTest {
    private val recipientA = WarmupInstanceId("recipient-a")
    private val recipientB = WarmupInstanceId("recipient-b")
    private val mappingA = WarmupTopicMapping(WarmupConsumerId("orders"), "orders", "warmup-orders")
    private val mappingB = WarmupTopicMapping(WarmupConsumerId("batches"), "batches", "warmup-batches")
    private val fingerprint = "fingerprint"
    private var now = 1_000L

    @Test
    fun `identical offset prefix produces identical session and reservation state`() {
        val first = WarmupSessionTable { now }
        val second = WarmupSessionTable { now }
        val session = WarmupSessionId("session-a")
        val reservation = WarmupReservationId("reservation-a")
        val records = listOf(
            record(0, request(session, recipientA)),
            record(1, reservation(session, recipientA, reservation, assignment(mappingA, 2))),
            record(2, ready(session, recipientA, assignment(mappingA, 2)))
        )

        records.forEach { first.apply(it) }
        records.forEach { second.apply(it) }
        first.markBootstrapAuthoritative(2)
        second.markBootstrapAuthoritative(2)

        assertEquals(first.snapshot(), second.snapshot())
        assertEquals(setOf(TopicPartition("warmup-orders", 2)), first.reservedPartitions())
    }

    @Test
    fun `first control offset atomically wins complete partition set`() {
        val table = WarmupSessionTable { now }
        val firstSession = WarmupSessionId("first")
        val secondSession = WarmupSessionId("second")
        table.apply(record(0, request(firstSession, recipientA)))
        table.apply(record(1, request(secondSession, recipientB)))

        val first = table.apply(
            record(
                2,
                reservation(
                    firstSession, recipientA, WarmupReservationId("r1"),
                    assignment(mappingA, 0), assignment(mappingB, 2)
                )
            )
        )
        val losing = table.apply(
            record(
                3,
                reservation(
                    secondSession, recipientB, WarmupReservationId("r2"),
                    assignment(mappingA, 1), assignment(mappingB, 2)
                )
            )
        )

        assertEquals(WarmupTableDecisionKind.RESERVATION_WON, first.kind)
        assertEquals(WarmupTableDecisionKind.RESERVATION_CONFLICT, losing.kind)
        assertEquals(
            setOf(TopicPartition("warmup-orders", 0), TopicPartition("warmup-batches", 2)),
            table.reservedPartitions()
        )
        assertNull(table.reservationFor(secondSession)?.reservationId)
    }

    @Test
    fun `loser can retry another non aligned destination set`() {
        val table = WarmupSessionTable { now }
        val firstSession = WarmupSessionId("first")
        val secondSession = WarmupSessionId("second")
        table.apply(record(0, request(firstSession, recipientA)))
        table.apply(record(1, request(secondSession, recipientB)))
        table.apply(record(2, reservation(firstSession, recipientA, WarmupReservationId("r1"), assignment(mappingA, 0))))
        table.apply(record(3, reservation(secondSession, recipientB, WarmupReservationId("r2"), assignment(mappingA, 0))))

        val retry = table.apply(
            record(4, reservation(secondSession, recipientB, WarmupReservationId("r3"), assignment(mappingA, 3)))
        )

        assertEquals(WarmupTableDecisionKind.RESERVATION_WON, retry.kind)
        assertEquals(
            setOf(TopicPartition("warmup-orders", 0), TopicPartition("warmup-orders", 3)),
            table.reservedPartitions()
        )
    }

    @Test
    fun `snapshot boundary applies contiguous later events and rejects a gap`() {
        val source = WarmupSessionTable { now }
        val session = WarmupSessionId("session")
        source.apply(record(4, request(session, recipientA)))
        source.markBootstrapAuthoritative(4)
        val snapshot = requireNotNull(source.snapshot())
        val next = record(5, reservation(session, recipientA, WarmupReservationId("reservation"), assignment(mappingA, 1)))

        val installed = WarmupSessionTable { now }
        assertTrue(installed.install(snapshot, listOf(next)))
        assertEquals(setOf(TopicPartition("warmup-orders", 1)), installed.reservedPartitions())

        val gapped = WarmupSessionTable { now }
        assertFalse(gapped.install(snapshot, listOf(next.copy(offset = 6))))
        assertFalse(gapped.isAuthoritative())
    }

    @Test
    fun `late snapshot cannot roll state back or restore closed reservation`() {
        val source = WarmupSessionTable { now }
        val session = WarmupSessionId("session")
        val reservation = WarmupReservationId("reservation")
        source.apply(record(0, request(session, recipientA)))
        source.apply(record(1, reservation(session, recipientA, reservation, assignment(mappingA, 0))))
        source.markBootstrapAuthoritative(1)
        val old = requireNotNull(source.snapshot())
        source.apply(record(2, release(session, recipientA, reservation)))

        assertFalse(source.install(old, emptyList()))
        assertTrue(source.reservedPartitions().isEmpty())
    }

    @Test
    fun `terminal session event retains ownership until exact reservation release`() {
        val table = WarmupSessionTable { now }
        val session = WarmupSessionId("session")
        val reservation = WarmupReservationId("reservation")
        table.apply(record(0, request(session, recipientA)))
        table.apply(record(1, reservation(session, recipientA, reservation, assignment(mappingA, 0))))

        table.apply(
            record(2, SessionEnded(session, recipientA, WarmupSessionEndState.COMPLETED, null, fingerprint, 2_000))
        )
        assertEquals(setOf(TopicPartition("warmup-orders", 0)), table.reservedPartitions())

        table.apply(record(3, release(session, recipientA, WarmupReservationId("stale"))))
        assertEquals(setOf(TopicPartition("warmup-orders", 0)), table.reservedPartitions())

        table.apply(record(4, release(session, recipientA, reservation)))
        assertTrue(table.reservedPartitions().isEmpty())
    }

    @Test
    fun `expiry releases ownership and stale release cannot free its replacement`() {
        val table = WarmupSessionTable { now }
        val oldSession = WarmupSessionId("old")
        val newSession = WarmupSessionId("new")
        val oldReservation = WarmupReservationId("old-r")
        val newReservation = WarmupReservationId("new-r")
        table.apply(record(0, request(oldSession, recipientA, expiresAt = 1_100)))
        table.apply(record(1, reservation(oldSession, recipientA, oldReservation, assignment(mappingA, 0), expiresAt = 1_100)))
        now = 1_101
        table.apply(record(2, request(newSession, recipientB, expiresAt = 2_000)))
        table.apply(record(3, reservation(newSession, recipientB, newReservation, assignment(mappingA, 0), expiresAt = 2_000)))

        table.apply(record(4, release(oldSession, recipientA, oldReservation, expiresAt = 2_000)))

        assertEquals(setOf(TopicPartition("warmup-orders", 0)), table.reservedPartitions())
        assertEquals(newReservation, table.reservationFor(newSession)?.reservationId)
    }

    @Test
    fun `empty state is not authoritative until safe bootstrap is explicitly completed`() {
        val table = WarmupSessionTable { now }
        assertNull(table.snapshot())
        table.markBootstrapAuthoritative(-1)
        assertEquals(WarmupStateSnapshot(-1, emptyList()), table.snapshot())
    }

    private fun request(
        session: WarmupSessionId,
        recipient: WarmupInstanceId,
        expiresAt: Long = 2_000
    ) = WarmupRequested(session, recipient, listOf(mappingA), minOf(1_500, expiresAt), fingerprint, expiresAt)

    private fun reservation(
        session: WarmupSessionId,
        recipient: WarmupInstanceId,
        reservation: WarmupReservationId,
        vararg assignments: WarmupPartitionAssignment,
        expiresAt: Long = 2_000
    ) = ReservationRequested(session, recipient, reservation, assignments.toList(), fingerprint, expiresAt)

    private fun release(
        session: WarmupSessionId,
        recipient: WarmupInstanceId,
        reservation: WarmupReservationId,
        expiresAt: Long = 2_000
    ) = ReservationReleased(session, recipient, reservation, fingerprint, expiresAt)

    private fun ready(
        session: WarmupSessionId,
        recipient: WarmupInstanceId,
        assignment: WarmupPartitionAssignment
    ) = RecipientReady(session, recipient, WarmupInstanceId("donor"), listOf(assignment), fingerprint, 2_000)

    private fun assignment(mapping: WarmupTopicMapping, partition: Int) =
        WarmupPartitionAssignment(mapping, partition)

    private fun record(offset: Long, message: WarmupControlMessage) = WarmupControlRecord(offset, message)
}
