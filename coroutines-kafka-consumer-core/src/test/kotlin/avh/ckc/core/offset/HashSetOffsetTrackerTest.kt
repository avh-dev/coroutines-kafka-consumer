package avh.ckc.core.offset

class HashSetOffsetTrackerTest : AbstractOffsetTrackerTest() {
    override fun createOffsetTracker(baseOffset: Long): AbstractOffsetTracker {
        return object : AbstractOffsetTracker {
            val delegate = HashSetOffsetTracker(baseOffset)
            override fun markProcessed(offset: Long) = delegate.markProcessed(offset)
            override fun advanceCommitOffset() = delegate.advanceCommitOffset()
        }
    }
}