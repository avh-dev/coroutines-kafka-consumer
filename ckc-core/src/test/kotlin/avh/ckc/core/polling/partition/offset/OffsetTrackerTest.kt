package avh.ckc.core.polling.partition.offset

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class OffsetTrackerTest : AbstractOffsetTrackerTest() {
    override fun createOffsetTracker(baseOffset: Long): AbstractOffsetTracker = object : AbstractOffsetTracker {
        val delegate = OffsetTracker(baseOffset)
        override fun markProcessed(offset: Long) = delegate.markProcessed(offset)
        override fun advanceCommitOffset(): Long? {
            val previousOffset = delegate.lastProcessedOffset
            delegate.advanceProcessedFrontier()
            return delegate.lastProcessedOffset.takeIf { it > previousOffset }
        }
    }

    @Test
    fun `snapshot restores processed offsets after a partial word commit`() {
        val tracker = OffsetTracker(initialProcessedOffset = 9L)
        tracker.markProcessed(10L)
        tracker.markProcessed(12L)
        tracker.markProcessed(13L)

        tracker.advanceProcessedFrontier()
        assertEquals(10L, tracker.lastProcessedOffset)

        val restored = OffsetTracker(
            initialProcessedOffset = tracker.lastProcessedOffset,
            snapshot = tracker.snapshotRoundTrip()
        )

        restored.advanceProcessedFrontier()
        assertEquals(10L, restored.lastProcessedOffset)
        restored.markProcessed(11L)
        restored.advanceProcessedFrontier()
        assertEquals(13L, restored.lastProcessedOffset)
    }

    @Test
    fun `snapshot restores offsets after the ring head advances`() {
        val tracker = OffsetTracker(initialProcessedOffset = -1L, initialCapacity = 128)
        for (offset in 0L until 128L) {
            tracker.markProcessed(offset)
        }
        tracker.advanceProcessedFrontier()
        assertEquals(127L, tracker.lastProcessedOffset)

        tracker.markProcessed(130L)
        tracker.markProcessed(131L)

        val restored = OffsetTracker(
            initialProcessedOffset = tracker.lastProcessedOffset,
            snapshot = tracker.snapshotRoundTrip()
        )

        restored.advanceProcessedFrontier()
        assertEquals(127L, restored.lastProcessedOffset)
        restored.markProcessed(128L)
        restored.markProcessed(129L)
        restored.advanceProcessedFrontier()
        assertEquals(131L, restored.lastProcessedOffset)
    }

    @Test
    fun `snapshot restore preserves grown capacity`() {
        val tracker = OffsetTracker(initialProcessedOffset = -1L, initialCapacity = 128)
        tracker.markProcessed(1_000L)

        val restored = OffsetTracker(
            initialProcessedOffset = tracker.lastProcessedOffset,
            snapshot = tracker.snapshotRoundTrip()
        )

        assertEquals(tracker.bitCapacity, restored.bitCapacity)
    }

    @Test
    fun `isProcessed returns true for contiguous and marked offsets`() {
        val tracker = OffsetTracker(initialProcessedOffset = 9L)

        tracker.markProcessed(10L)
        tracker.markProcessed(12L)

        assertTrue(tracker.isProcessed(9L))
        assertTrue(tracker.isProcessed(10L))
        assertFalse(tracker.isProcessed(11L))
        assertTrue(tracker.isProcessed(12L))
    }

    @Test
    fun `isProcessed returns false for offsets outside current ring`() {
        val tracker = OffsetTracker(initialProcessedOffset = 9L, initialCapacity = 128)

        assertFalse(tracker.isProcessed(10_000L))
    }

    private fun OffsetTracker.snapshotRoundTrip(): OffsetTrackerSnapshot =
        OffsetTrackerSerializer.deserialize(OffsetTrackerSerializer.serialize(snapshot()))
}
