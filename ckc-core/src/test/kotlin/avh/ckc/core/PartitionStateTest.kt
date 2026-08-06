package avh.ckc.core

import avh.ckc.core.polling.partition.PartitionState
import avh.ckc.core.polling.partition.offset.OffsetTracker
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class PartitionStateTest {

    @Test
    fun `when initialized from sentinel then tracker is reset`() {
        val ps = PartitionState(TopicPartition("t", 0))

        val before = ps.trackerRefForTest()
        ps.init(42L)
        val after = ps.trackerRefForTest()

        assertNotSame(before, after)
    }

    @Test
    fun `when initialized with same position then tracker is not reset`() {
        val ps = PartitionState(TopicPartition("t", 0))

        ps.init(100L)
        val trackerBefore = ps.trackerRefForTest()

        ps.init(100L)
        val trackerAfter = ps.trackerRefForTest()

        assertSame(trackerBefore, trackerAfter)
    }

    @Test
    fun `when position jumps forward then tracker is reset`() {
        val ps = PartitionState(TopicPartition("t", 0))

        ps.init(100L)
        val trackerBefore = ps.trackerRefForTest()

        ps.init(1000L)
        val trackerAfter = ps.trackerRefForTest()

        assertNotSame(trackerBefore, trackerAfter)
    }

    @Test
    fun `when tracker grows then partition stats expose bit capacity`() {
        val ps = PartitionState(TopicPartition("t", 3))

        ps.init(0L)
        ps.markProcessed(256L)

        assertEquals("t", ps.topic)
        assertEquals(3, ps.partition)
        assertEquals(512, ps.offsetTrackerBitCapacity)
    }

    @Test
    fun `when initialized from snapshot then processed offsets are restored`() {
        val original = OffsetTracker(initialProcessedOffset = 9L)
        original.markProcessed(10L)
        original.markProcessed(12L)
        original.advanceProcessedFrontier()
        assertEquals(10L, original.lastProcessedOffset)

        val ps = PartitionState(TopicPartition("t", 0))
        ps.init(committedOffset = original.lastProcessedOffset + 1, snapshot = original.snapshot())

        assertTrue(ps.isProcessed(10L))
        assertFalse(ps.isProcessed(11L))
        assertTrue(ps.isProcessed(12L))
    }

    @Test
    fun `when initialized from snapshot with same committed offset then current tracker is preserved`() {
        val staleSnapshotSource = OffsetTracker(initialProcessedOffset = 9L)
        staleSnapshotSource.markProcessed(12L)

        val ps = PartitionState(TopicPartition("t", 0))
        ps.init(10L)
        ps.markProcessed(13L)
        val trackerBefore = ps.trackerRefForTest()

        ps.init(committedOffset = 10L, snapshot = staleSnapshotSource.snapshot())

        assertSame(trackerBefore, ps.trackerRefForTest())
        assertTrue(ps.isProcessed(13L))
        assertFalse(ps.isProcessed(12L))
    }

    @Test
    fun `commit data includes snapshot after offset advancement`() {
        val ps = PartitionState(TopicPartition("t", 0))
        ps.init(10L)
        ps.markProcessed(10L)
        ps.markProcessed(12L)

        val commitData = ps.advanceAndGetCommitData()
        assertNotNull(commitData)

        assertEquals(10L, commitData!!.offset)
        assertEquals(1L, commitData.advancedOffsetsCount)

        val restored = OffsetTracker(
            initialProcessedOffset = commitData.offset,
            snapshot = commitData.offsetTrackerSnapshot
        )
        assertTrue(restored.isProcessed(12L))
    }

    @Test
    fun `commit data remains available until successful commit is confirmed`() {
        val ps = PartitionState(TopicPartition("t", 0))
        ps.init(10L)
        ps.markProcessed(10L)

        val firstAttempt = ps.advanceAndGetCommitData()
        val retryAttempt = ps.advanceAndGetCommitData()

        assertEquals(10L, firstAttempt?.offset)
        assertEquals(1L, firstAttempt?.advancedOffsetsCount)
        assertEquals(10L, retryAttempt?.offset)
        assertEquals(1L, retryAttempt?.advancedOffsetsCount)

        ps.markCommitted(10L)

        assertNull(ps.advanceAndGetCommitData())
        assertEquals(10L, ps.lastCommittedOffset)
    }

    @Test
    fun `commit data compacts oversized tracker before snapshot`() {
        val ps = PartitionState(TopicPartition("t", 0))
        ps.init(0L)
        ps.markProcessed(16_384L)
        for (offset in 0L until 16_384L) {
            ps.markProcessed(offset)
        }
        assertEquals(32_768, ps.offsetTrackerBitCapacity)

        val commitData = checkNotNull(ps.advanceAndGetCommitData())

        assertEquals(16_384L, commitData.offset)
        assertEquals(128, ps.offsetTrackerBitCapacity)
        assertEquals(2, commitData.offsetTrackerSnapshot.words.size)
    }

    @Test
    fun `commit data keeps normal one kilobyte tracker capacity`() {
        val ps = PartitionState(TopicPartition("t", 0))
        ps.init(0L)
        ps.markProcessed(4_096L)
        for (offset in 0L until 4_096L) {
            ps.markProcessed(offset)
        }
        assertEquals(8_192, ps.offsetTrackerBitCapacity)

        val commitData = checkNotNull(ps.advanceAndGetCommitData())

        assertEquals(4_096L, commitData.offset)
        assertEquals(8_192, ps.offsetTrackerBitCapacity)
        assertEquals(128, commitData.offsetTrackerSnapshot.words.size)
    }
}
