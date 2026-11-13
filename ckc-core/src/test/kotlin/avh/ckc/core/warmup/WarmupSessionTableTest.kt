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
    @Test
    fun `identical offset prefix produces identical session and reservation state`() {
        val first = WarmupSessionTable()
        val second = WarmupSessionTable()
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
    fun `reading the same prefix on opposite sides of expiry keeps the same winner`() {
        val beforeExpiry = WarmupSessionTable()
        val afterExpiry = WarmupSessionTable()
        val expiresAt = System.currentTimeMillis() + 100
        val firstSession = WarmupSessionId("first")
        val secondSession = WarmupSessionId("second")
        val records = listOf(
            record(0, request(firstSession, recipientA, expiresAt)),
            record(1, request(secondSession, recipientB, expiresAt)),
            record(2, reservation(
                firstSession, recipientA, WarmupReservationId("first-r"), assignment(mappingA, 0),
                expiresAt = expiresAt
            )),
            record(3, reservation(
                secondSession, recipientB, WarmupReservationId("second-r"), assignment(mappingA, 0),
                expiresAt = expiresAt
            ))
        )

        val earlyDecisions = records.map(beforeExpiry::apply)
        Thread.sleep(150)
        val lateDecisions = records.map(afterExpiry::apply)
        beforeExpiry.markBootstrapAuthoritative(3)
        afterExpiry.markBootstrapAuthoritative(3)

        assertEquals(earlyDecisions.map { it.kind }, lateDecisions.map { it.kind })
        assertEquals(WarmupTableDecisionKind.RESERVATION_CONFLICT, lateDecisions.last().kind)
        assertEquals(beforeExpiry.snapshot(), afterExpiry.snapshot())
    }

    @Test
    fun `first control offset atomically wins complete partition set`() {
        val table = WarmupSessionTable()
        val firstSession = WarmupSessionId("first")
        val secondSession = WarmupSessionId("second")
        table.apply(record(0, request(firstSession, recipientA)))
        table.apply(record(1, request(secondSession, recipientB)))

        val firstRequest = reservation(
            firstSession, recipientA, WarmupReservationId("r1"),
            assignment(mappingA, 0), assignment(mappingB, 2)
        )
        val first = table.apply(record(2, firstRequest))
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
        assertEquals(WarmupTableDecisionKind.DUPLICATE, table.apply(record(4, firstRequest)).kind)
        assertEquals(
            setOf(TopicPartition("warmup-orders", 0), TopicPartition("warmup-batches", 2)),
            table.reservedPartitions()
        )
        assertNull(table.reservationFor(secondSession)?.reservationId)
    }

    @Test
    fun `loser can retry another non aligned destination set`() {
        val table = WarmupSessionTable()
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
    fun `stale fingerprint and deadline cannot mutate a live session`() {
        val table = WarmupSessionTable()
        val session = WarmupSessionId("session")
        table.apply(record(0, request(session, recipientA)))
        val candidate = reservation(
            session, recipientA, WarmupReservationId("reservation"), assignment(mappingA, 0)
        )

        assertEquals(
            WarmupTableDecisionKind.IGNORED_INVALID,
            table.apply(record(1, candidate.copy(configurationFingerprint = "stale"))).kind
        )
        assertEquals(
            WarmupTableDecisionKind.IGNORED_INVALID,
            table.apply(record(2, candidate.copy(expiresAtEpochMillis = 2_001))).kind
        )
        assertTrue(table.reservedPartitions().isEmpty())
    }

    @Test
    fun `snapshot boundary applies contiguous later events and rejects a gap`() {
        val source = WarmupSessionTable()
        val session = WarmupSessionId("session")
        source.apply(record(4, request(session, recipientA)))
        source.markBootstrapAuthoritative(4)
        val snapshot = requireNotNull(source.snapshot())
        val next = record(5, reservation(session, recipientA, WarmupReservationId("reservation"), assignment(mappingA, 1)))

        val installed = WarmupSessionTable()
        assertTrue(installed.install(snapshot, listOf(next)))
        assertEquals(setOf(TopicPartition("warmup-orders", 1)), installed.reservedPartitions())

        val gapped = WarmupSessionTable()
        assertFalse(gapped.install(snapshot, listOf(next.copy(offset = 6))))
        assertFalse(gapped.isAuthoritative())
    }

    @Test
    fun `late snapshot cannot roll state back or restore closed reservation`() {
        val source = WarmupSessionTable()
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
        val table = WarmupSessionTable()
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
    fun `only explicit safe emergency release frees expired ownership and stale release cannot free replacement`() {
        val table = WarmupSessionTable()
        val oldSession = WarmupSessionId("old")
        val newSession = WarmupSessionId("new")
        val oldReservation = WarmupReservationId("old-r")
        val newReservation = WarmupReservationId("new-r")
        table.apply(record(0, request(oldSession, recipientA, expiresAt = 1_100)))
        table.apply(record(1, reservation(oldSession, recipientA, oldReservation, assignment(mappingA, 0), expiresAt = 1_100)))
        table.apply(record(2, request(newSession, recipientB, expiresAt = 2_000)))
        assertEquals(
            WarmupTableDecisionKind.RESERVATION_CONFLICT,
            table.apply(record(3, reservation(newSession, recipientB, newReservation, assignment(mappingA, 0), expiresAt = 2_000))).kind
        )

        assertEquals(
            WarmupTableDecisionKind.IGNORED_INVALID,
            table.apply(record(4, emergencyRelease(oldSession, recipientB, oldReservation, 1_100, 1_100))).kind
        )
        table.apply(record(5, emergencyRelease(
            oldSession, recipientB, oldReservation, 1_100, 1_100 + WARMUP_MAX_PAIRWISE_CLOCK_SKEW_MILLIS
        )))
        assertEquals(
            WarmupTableDecisionKind.RESERVATION_WON,
            table.apply(record(6, reservation(newSession, recipientB, newReservation, assignment(mappingA, 0), expiresAt = 2_000))).kind
        )

        table.apply(record(7, release(oldSession, recipientA, oldReservation, expiresAt = 1_100)))

        assertEquals(setOf(TopicPartition("warmup-orders", 0)), table.reservedPartitions())
        assertEquals(newReservation, table.reservationFor(newSession)?.reservationId)
    }

    @Test
    fun `empty state is not authoritative until safe bootstrap is explicitly completed`() {
        val table = WarmupSessionTable()
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
    ) = ReservationReleased(
        session, recipient, reservation, WarmupReservationReleaseKind.NORMAL, 1_500, fingerprint, expiresAt
    )

    private fun emergencyRelease(
        session: WarmupSessionId,
        sender: WarmupInstanceId,
        reservation: WarmupReservationId,
        expiresAt: Long,
        releasedAt: Long
    ) = ReservationReleased(
        session, sender, reservation, WarmupReservationReleaseKind.EMERGENCY_TIMEOUT,
        releasedAt, fingerprint, expiresAt
    )

    private fun ready(
        session: WarmupSessionId,
        recipient: WarmupInstanceId,
        assignment: WarmupPartitionAssignment
    ) = RecipientReady(session, recipient, WarmupInstanceId("donor"), listOf(assignment), fingerprint, 2_000)

    private fun assignment(mapping: WarmupTopicMapping, partition: Int) =
        WarmupPartitionAssignment(mapping, partition)

    private fun record(offset: Long, message: WarmupControlMessage) = WarmupControlRecord(offset, message)
}
