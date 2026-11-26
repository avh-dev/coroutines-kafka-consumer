package avh.ckc.core

import avh.ckc.core.polling.partition.offset.AbstractOffsetTracker
import avh.ckc.core.polling.partition.offset.AbstractOffsetTrackerTest

class HashSetOffsetTrackerTest : AbstractOffsetTrackerTest() {
    override fun createOffsetTracker(baseOffset: Long): AbstractOffsetTracker {
        return object : AbstractOffsetTracker {
            val delegate = HashSetOffsetTracker(baseOffset)
            override fun markProcessed(offset: Long) = delegate.markProcessed(offset)
            override fun advanceCommitOffset() = delegate.advanceCommitOffset()
        }
    }
}