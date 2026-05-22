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
        val original = OffsetTracker(lastCommitedOffset = 9L)
        original.markProcessed(10L)
        original.markProcessed(12L)
        assertEquals(10L, original.advanceCommitOffset())

        val ps = PartitionState(TopicPartition("t", 0))
        ps.init(committedOffset = original.lastCommitedOffset + 1, snapshot = original.snapshot())

        assertTrue(ps.isProcessed(10L))
        assertFalse(ps.isProcessed(11L))
        assertTrue(ps.isProcessed(12L))
    }

    @Test
    fun `when initialized from snapshot with same committed offset then current tracker is preserved`() {
        val staleSnapshotSource = OffsetTracker(lastCommitedOffset = 9L)
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

        val restored = OffsetTracker(lastCommitedOffset = commitData.offset, snapshot = commitData.offsetTrackerSnapshot)
        assertTrue(restored.isProcessed(12L))
    }
}
