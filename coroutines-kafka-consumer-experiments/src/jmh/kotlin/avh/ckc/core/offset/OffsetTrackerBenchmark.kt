package avh.ckc.core.offset

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Warmup
import java.util.concurrent.TimeUnit
import kotlin.random.Random

/**
 * JMH benchmark for different OffsetTracker implementations.
 *
 */
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
open class OffsetTrackerBenchmark {

    interface OffsetTracker {
        fun markProcessed(offset: Long)
        fun advanceCommitOffset(): Long?
    }

    /**
     * Implementation to benchmark.
     *
     * Adjust names to match your actual classes.
     */
    @Param("hashSet", "slidingBitset", "ringBitset")
    lateinit var impl: String

    /**
     * Window size: how many offsets we process per run.
     */
    @Param("1000000")
    var offsetsCount: Int = 0

    @Param("64")
    var initialCapacity: Int = 0

    /**
     * Window size: how many offsets we process per run.
     */
    @Param("true")
    var isRandom: Boolean = true

    private lateinit var sequentialOffsets: LongArray
    private lateinit var randomOffsets: LongArray

    @Setup(Level.Trial)
    fun setupData() {
        // offsets 1..windowSize
        sequentialOffsets = LongArray(offsetsCount) { i -> (i + 1).toLong() }
        randomOffsets = shuffleLocal(offsetsCount, 64, Random(12345))
    }

    // -------------------------------------------------------------------------
    // Benchmarks
    // -------------------------------------------------------------------------

    @Benchmark
    fun periodicAdvanceCommit(): Long {
        val tracker = newTracker()
        var q = 0
        var t: Long? = null
        val offsets = if (isRandom) randomOffsets else sequentialOffsets
        for (offset in offsets) {
            tracker.markProcessed(offset)
            if (q++ and 15 == 15) {
                t = tracker.advanceCommitOffset()
            }
        }
        return t ?: 0L
    }

    @Benchmark
    fun oneAdvanceCommit(): Long {
        val tracker = newTracker()
        var t: Long? = null
        val offsets = if (isRandom) randomOffsets else sequentialOffsets
        for (offset in offsets) {
            tracker.markProcessed(offset)
        }
        t = tracker.advanceCommitOffset()
        return t ?: 0L
    }


    @Benchmark
    fun everyAdvanceCommit(): Long {
        val tracker = newTracker()
        var t: Long? = null
        val offsets = if (isRandom) randomOffsets else sequentialOffsets
        for (offset in offsets) {
            tracker.markProcessed(offset)
            t = tracker.advanceCommitOffset()
        }
        return t ?: 0L
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private fun newTracker(): OffsetTracker =
        when (impl) {
            "hashSet" -> object : OffsetTracker {
                val delegate = HashSetOffsetTracker(lastCommitOffset = 0L)
                override fun markProcessed(offset: Long) =  delegate.markProcessed(offset)
                override fun advanceCommitOffset() = delegate.advanceCommitOffset()
            }
            "slidingBitset" -> object : OffsetTracker {
                val delegate = SlidingBitsetOffsetTracker(lastCommit = -1L, initialCapacity)
                override fun markProcessed(offset: Long) =  delegate.markProcessed(offset)
                override fun advanceCommitOffset() = delegate.advanceCommitOffset()
            }
            "ringBitset" -> object : OffsetTracker {
                val delegate = RingBitsetOffsetTracker(lastCommitedOffset = -1L, initialCapacity)
                override fun markProcessed(offset: Long) =  delegate.markProcessed(offset)
                override fun advanceCommitOffset() = delegate.advanceCommitOffset()
            }
            else -> error("Unknown impl: $impl")
        }
}