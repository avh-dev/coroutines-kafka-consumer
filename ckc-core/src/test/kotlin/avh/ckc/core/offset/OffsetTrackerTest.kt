package avh.ckc.core.offset

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class OffsetTrackerTest : AbstractOffsetTrackerTest() {
    override fun createOffsetTracker(baseOffset: Long): AbstractOffsetTracker = object : AbstractOffsetTracker {
        val delegate = OffsetTracker(baseOffset)
        override fun markProcessed(offset: Long) = delegate.markProcessed(offset)
        override fun advanceCommitOffset() = delegate.advanceCommitOffset()
    }

    @Test
    fun `snapshot restores processed offsets after a partial word commit`() {
        val tracker = OffsetTracker(lastCommitedOffset = 9L)
        tracker.markProcessed(10L)
        tracker.markProcessed(12L)
        tracker.markProcessed(13L)

        assertEquals(10L, tracker.advanceCommitOffset())

        val restored = OffsetTracker(
            lastCommitedOffset = tracker.lastCommitedOffset,
            snapshot = tracker.snapshotRoundTrip()
        )

        assertNull(restored.advanceCommitOffset())
        restored.markProcessed(11L)
        assertEquals(13L, restored.advanceCommitOffset())
    }

    @Test
    fun `snapshot restores offsets after the ring head advances`() {
        val tracker = OffsetTracker(lastCommitedOffset = -1L, initialCapacity = 128)
        for (offset in 0L until 128L) {
            tracker.markProcessed(offset)
        }
        assertEquals(127L, tracker.advanceCommitOffset())

        tracker.markProcessed(130L)
        tracker.markProcessed(131L)

        val restored = OffsetTracker(
            lastCommitedOffset = tracker.lastCommitedOffset,
            snapshot = tracker.snapshotRoundTrip()
        )

        assertNull(restored.advanceCommitOffset())
        restored.markProcessed(128L)
        restored.markProcessed(129L)
        assertEquals(131L, restored.advanceCommitOffset())
    }

    @Test
    fun `snapshot restore preserves grown capacity`() {
        val tracker = OffsetTracker(lastCommitedOffset = -1L, initialCapacity = 128)
        tracker.markProcessed(1_000L)

        val restored = OffsetTracker(
            lastCommitedOffset = tracker.lastCommitedOffset,
            snapshot = tracker.snapshotRoundTrip()
        )

        assertEquals(tracker.bitCapacity, restored.bitCapacity)
    }

    private fun OffsetTracker.snapshotRoundTrip(): OffsetTrackerSnapshot =
        OffsetTrackerSerializer.deserialize(OffsetTrackerSerializer.serialize(snapshot()))
}
