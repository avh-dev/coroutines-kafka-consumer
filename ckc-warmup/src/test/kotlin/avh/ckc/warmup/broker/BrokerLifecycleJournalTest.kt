package avh.ckc.warmup.broker

import avh.ckc.warmup.broker.BrokerLifecycleEvent.ContextDrained
import avh.ckc.warmup.broker.BrokerLifecycleEvent.DrainAccepted
import avh.ckc.warmup.broker.BrokerLifecycleEvent.DrainRequested
import avh.ckc.warmup.broker.BrokerLifecycleEvent.JoinConfirmed
import avh.ckc.warmup.broker.BrokerLifecycleEvent.Joined
import avh.ckc.warmup.broker.BrokerLifecycleEvent.Left
import avh.ckc.warmup.broker.BrokerLifecycleEvent.Started
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class BrokerLifecycleJournalTest {
    @Test
    fun `first request freezes membership and ignores late duplicates and stale confirmations`() {
        val journal = BrokerLifecycleJournal()
        val request = DrainRequested("current", "first", 1_000)
        listOf(
            Started("old"),
            Joined("old", "old-instance", "old-registration"),
            Started("current"),
            Joined("current", "leaving-before", "registration-a"),
            Joined("current", "leaving-after", "registration-b"),
            Joined("current", "crashed", "registration-c"),
            Left("current", "leaving-before"),
            request,
            DrainRequested("current", "second", 2_000),
            Left("current", "leaving-after"),
            Joined("current", "joining-after", "registration-d"),
            DrainAccepted("current", "first", "crashed"),
            ContextDrained("old", "first", "crashed", emptySet()),
            ContextDrained("current", "second", "crashed", emptySet()),
            ContextDrained("current", "first", "joining-after", emptySet()),
            ContextDrained("current", "first", "leaving-after", setOf("session-1")),
            ContextDrained("current", "first", "leaving-after", setOf("session-1")),
            ContextDrained("current", "first", "crashed", emptySet())
        ).forEachIndexed { offset, event ->
            val decoded = BrokerLifecycleCodec.decode(BrokerLifecycleCodec.encode(event))
            assertEquals(event, decoded)
            val timestamp = if (offset == 17) 1_001L else 0L
            journal.accept(BrokerLifecycleRecord(offset.toLong(), decoded, timestamp))
        }
        val drain = checkNotNull(journal.drain)
        assertEquals(7L, drain.offset)
        assertEquals(request, drain.request)
        assertEquals(setOf("leaving-after", "crashed"), drain.expected)
        assertEquals(setOf("crashed"), drain.accepted)
        assertEquals(setOf("crashed"), drain.missing)
    }

    @Test
    fun `a missing membership offset invalidates an epoch until a fresh marker`() {
        val journal = BrokerLifecycleJournal()
        journal.accept(BrokerLifecycleRecord(5, Started("incomplete")))
        journal.accept(BrokerLifecycleRecord(7, DrainRequested("incomplete", "drain", 1_000)))
        assertNull(journal.epoch)
        assertNull(journal.drain)

        journal.accept(BrokerLifecycleRecord(8, Started("fresh")))
        journal.accept(BrokerLifecycleRecord(9, JoinConfirmed("incomplete", "stale", "stale")))
        journal.accept(BrokerLifecycleRecord(10, DrainRequested("fresh", "drain", 2_000)))
        assertEquals("fresh", journal.epoch)
        assertEquals(emptySet(), journal.drain?.expected)
    }
}
