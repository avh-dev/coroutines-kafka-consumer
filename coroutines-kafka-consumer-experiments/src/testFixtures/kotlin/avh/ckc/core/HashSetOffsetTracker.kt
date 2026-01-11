package avh.ckc.core

/*
 Reference implementation of offset tracker for testing and performance testing
 */

class HashSetOffsetTracker(
    private var lastCommitOffset: Long,
    hashSetFactory: () -> MutableSet<Long> =  { HashSet() }
) {
    private var nextCommitOffset = lastCommitOffset + 1
    private val processed: MutableSet<Long> = hashSetFactory()

    fun markProcessed(offset: Long) {
        if (offset < nextCommitOffset) return
        processed.add(offset)
    }

    fun advanceCommitOffset(): Long? {
        var moved = false
        while (processed.remove(nextCommitOffset)) {
            nextCommitOffset++
            moved = true
        }
        if (!moved) return null
        val commitOffset = nextCommitOffset - 1
        lastCommitOffset = commitOffset
        return commitOffset
    }
}