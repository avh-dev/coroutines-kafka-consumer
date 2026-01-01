package avh.ckc.core.experiments.offset.tracker

import avh.ckc.core.avh.ckc.core.AbstractOffsetTrackerTest

class SlidingBitSetOffsetTrackerTest : AbstractOffsetTrackerTest() {
    override fun createOffsetTracker(baseOffset: Long): AbstractOffsetTracker = object : AbstractOffsetTracker {
        val delegate = SlidingBitsetOffsetTracker(baseOffset)
        override fun markProcessed(offset: Long) = delegate.markProcessed(offset)
        override fun advanceCommitOffset() = delegate.advanceCommitOffset()
    }
}