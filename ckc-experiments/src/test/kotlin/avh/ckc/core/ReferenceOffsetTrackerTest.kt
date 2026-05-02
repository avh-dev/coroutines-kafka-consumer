package avh.ckc.core

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals

class ReferenceOffsetTrackerTest {
    @Test
    fun `when the same input then the commited offset must be the same`() {

        val hashSetTracker = HashSetOffsetTracker(-1)
        val slidingBitsetTracker = SlidingBitsetOffsetTracker(-1)
        val ringBitsetOffsetTracker = RingBitsetOffsetTracker(-1)

        val offsets = shuffleLocal(1000000, 64, Random(12345))

        var q = 0
        for (offset in offsets) {
            hashSetTracker.markProcessed(offset)
            slidingBitsetTracker.markProcessed(offset)
            ringBitsetOffsetTracker.markProcessed(offset)
            if (q++ and 15 == 15) {
                val hashSetOffset = hashSetTracker.advanceCommitOffset()
                val slidingBitsetOffset = slidingBitsetTracker.advanceCommitOffset()
                val ringBitsetOffset = ringBitsetOffsetTracker.advanceCommitOffset()
                println(hashSetOffset)
                assertEquals(hashSetOffset, slidingBitsetOffset, "sliding")
                assertEquals(hashSetOffset, ringBitsetOffset, "ring")
            }
        }

    }
}