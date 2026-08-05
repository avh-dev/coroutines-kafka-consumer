package avh.ckc.core.polling.partition.offset

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class OffsetTrackerTest : AbstractOffsetTrackerTest() {
    override fun createOffsetTracker(baseOffset: Long): AbstractOffsetTracker = object : AbstractOffsetTracker {
        val delegate = OffsetTracker(baseOffset)
        override fun markProcessed(offset: Long) = delegate.markProcessed(offset)
        override fun advanceCommitOffset() = delegate.advanceProcessedOffset()
    }

    @Test
    fun `snapshot restores processed offsets after a partial word commit`() {
        val tracker = OffsetTracker(initialProcessedOffset = 9L)
        tracker.markProcessed(10L)
        tracker.markProcessed(12L)
        tracker.markProcessed(13L)

        assertEquals(10L, tracker.advanceProcessedOffset())

        val restored = OffsetTracker(
            initialProcessedOffset = tracker.lastProcessedOffset,
            snapshot = tracker.snapshotRoundTrip()
        )

        assertNull(restored.advanceProcessedOffset())
        restored.markProcessed(11L)
        assertEquals(13L, restored.advanceProcessedOffset())
    }

    @Test
    fun `snapshot restores offsets after the ring head advances`() {
        val tracker = OffsetTracker(initialProcessedOffset = -1L, initialCapacity = 128)
        for (offset in 0L until 128L) {
            tracker.markProcessed(offset)
        }
        assertEquals(127L, tracker.advanceProcessedOffset())

        tracker.markProcessed(130L)
        tracker.markProcessed(131L)

        val restored = OffsetTracker(
            initialProcessedOffset = tracker.lastProcessedOffset,
            snapshot = tracker.snapshotRoundTrip()
        )

        assertNull(restored.advanceProcessedOffset())
        restored.markProcessed(128L)
        restored.markProcessed(129L)
        assertEquals(131L, restored.advanceProcessedOffset())
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
