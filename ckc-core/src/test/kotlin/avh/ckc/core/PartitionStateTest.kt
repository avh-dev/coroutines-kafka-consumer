package avh.ckc.core

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
}