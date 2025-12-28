package avh.ckc.core.offset

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Contract test for OffsetTracker implementations.
 *
 * Each implementation must extend this class and provide [createOffsetTracker].
 */
abstract class AbstractOffsetTrackerTest {

    interface AbstractOffsetTracker {
        fun markProcessed(offset: Long)
        fun advanceCommitOffset(): Long?
    }

    /**
     * Factory method to create a new tracker with given baseOffset.
     */
    protected abstract fun createOffsetTracker(baseOffset: Long = 0L): AbstractOffsetTracker

    @Test
    fun `when nothing is processed then commit offset is null`() {
        // given
        val baseOffset = 10L
        val tracker = createOffsetTracker(baseOffset)

        // when
        val commitOffset = tracker.advanceCommitOffset()

        // then
        assertNull(commitOffset)
    }

    @Test
    fun `when offsets are processed sequentially then commit advances step by step`() {
        // given
        val tracker = createOffsetTracker(baseOffset = 10L)

        // when
        tracker.markProcessed(11L)
        val firstCommit = tracker.advanceCommitOffset()

        tracker.markProcessed(12L)
        val secondCommit = tracker.advanceCommitOffset()

        // then
        assertEquals(11L, firstCommit)
        assertEquals(12L, secondCommit)
        // and when nothing new is processed then there is nothing to commit
        assertNull(tracker.advanceCommitOffset())
    }

    @Test
    fun `when offsets are processed out of order then commit waits for missing ones`() {
        // given
        val tracker = createOffsetTracker(baseOffset = 10L)

        // when
        tracker.markProcessed(12L)
        val firstCommit = tracker.advanceCommitOffset()

        tracker.markProcessed(11L)
        val secondCommit = tracker.advanceCommitOffset()

        // then
        assertNull(firstCommit)

        assertEquals(12L, secondCommit)
    }

    @Test
    fun `when gap exists then commit stops before gap`() {
        // given
        val tracker = createOffsetTracker(baseOffset = 10L)

        // when
        tracker.markProcessed(11L)
        tracker.markProcessed(13L)

        val firstCommit = tracker.advanceCommitOffset()
        val secondCommit = tracker.advanceCommitOffset()

        // then
        // commit should advance only up to 11 because 12 is missing
        assertEquals(11L, firstCommit)

        // no progress until gap is closed
        assertNull(secondCommit)

        // when missing offset is processed
        tracker.markProcessed(12L)
        val thirdCommit = tracker.advanceCommitOffset()

        // then
        assertEquals(13L, thirdCommit)
    }

    @Test
    fun `when same offset is processed multiple times then it does not break the logic`() {
        // given
        val tracker = createOffsetTracker(baseOffset = 10L)

        // when
        tracker.markProcessed(11L)
        tracker.markProcessed(11L)
        tracker.markProcessed(11L)

        val firstCommit = tracker.advanceCommitOffset()
        val secondCommit = tracker.advanceCommitOffset()

        // then
        assertEquals(11L, firstCommit)
        assertNull(secondCommit)
    }

    @Test
    fun `when offset is below baseOffset plus one then it is ignored`() {
        // given
        val tracker = createOffsetTracker(baseOffset = 10L)

        // when
        tracker.markProcessed(9L)
        val firstCommit = tracker.advanceCommitOffset()

        tracker.markProcessed(11L)
        val secondCommit = tracker.advanceCommitOffset()

        // then
        assertNull(firstCommit)
        assertEquals(11L, secondCommit)
    }

    @Test
    fun `when committing multiple times then commit offsets are strictly increasing`() {
        // given
        val tracker = createOffsetTracker(baseOffset = 0L)

        // when
        val offsets = listOf(3L, 1L, 2L, 5L, 4L)
        offsets.forEach { tracker.markProcessed(it) }

        val commits = mutableListOf<Long>()
        while (true) {
            val c = tracker.advanceCommitOffset() ?: break
            commits += c
        }

        // then
        for (i in 1 until commits.size) {
            assertTrue(
                commits[i] > commits[i - 1],
                "Commit offsets must be strictly increasing: $commits"
            )
        }
    }

    @Test
    fun `when offsets cross 64-bit boundary relative to baseOffset then commit still advances correctly`() {
        // This test is important for implementations that use 64-bit words (Long) as bit storage.
        // It validates correct behavior around the boundary between two consecutive 64-bit chunks.
        //
        // Layout for baseOffset = 0:
        //   offsets 1..64  -> first 64-bit word
        //   offsets 65..128 -> second 64-bit word

        // given
        val tracker = createOffsetTracker(baseOffset = 0L)

        // when
        // Process 1..63 and 65, but skip 64
        for (offset in 1L..63L) {
            tracker.markProcessed(offset)
        }
        tracker.markProcessed(65L)

        val firstCommit = tracker.advanceCommitOffset()

        // 64 is still missing, so commit should stop at 63
        assertEquals(63L, firstCommit)

        // when
        // Now process the missing 64 (the last bit in the first 64-bit word)
        tracker.markProcessed(64L)
        val secondCommit = tracker.advanceCommitOffset()

        // then
        // Now both 64 and 65 are available, so commit must jump to 65,
        // crossing the 64-bit boundary between the first and the second word.
        assertEquals(65L, secondCommit)
    }
}