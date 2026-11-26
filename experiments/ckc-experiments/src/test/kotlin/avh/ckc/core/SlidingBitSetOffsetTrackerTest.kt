package avh.ckc.core

import avh.ckc.core.polling.partition.offset.AbstractOffsetTracker
import avh.ckc.core.polling.partition.offset.AbstractOffsetTrackerTest

class SlidingBitSetOffsetTrackerTest : AbstractOffsetTrackerTest() {
    override fun createOffsetTracker(baseOffset: Long): AbstractOffsetTracker = object : AbstractOffsetTracker {
        val delegate = SlidingBitsetOffsetTracker(baseOffset)
        override fun markProcessed(offset: Long) = delegate.markProcessed(offset)
        override fun advanceCommitOffset() = delegate.advanceCommitOffset()
    }
}