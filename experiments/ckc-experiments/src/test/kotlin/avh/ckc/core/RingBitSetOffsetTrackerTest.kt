package avh.ckc.core

import avh.ckc.core.polling.partition.offset.AbstractOffsetTracker
import avh.ckc.core.polling.partition.offset.AbstractOffsetTrackerTest

class RingBitSetOffsetTrackerTest : AbstractOffsetTrackerTest() {
    override fun createOffsetTracker(baseOffset: Long): AbstractOffsetTracker = object : AbstractOffsetTracker {
        val delegate = RingBitsetOffsetTracker(baseOffset)
        override fun markProcessed(offset: Long) = delegate.markProcessed(offset)
        override fun advanceCommitOffset() = delegate.advanceCommitOffset()
    }
}