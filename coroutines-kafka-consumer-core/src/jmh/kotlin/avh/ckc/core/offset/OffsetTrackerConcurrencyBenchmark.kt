package avh.ckc.core.offset

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.openjdk.jmh.annotations.*
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.random.Random

/**
 * "Real world" benchmark:
 *
 *  - Producer puts offsets 1..totalMessages into Channel<Long>
 *  - N worker coroutines read from the channel:
 *      * mark offset as processed
 *      * sleep random 1-5 ms
 *  - One commit coroutine:
 *      * every 10 ms calls advanceCommitOffset()
 *
 *  We compare synchronization strategies around SlidingBitsetOffsetTracker:
 *   - sync   : SynchronizedOffsetTracker (synchronized {})
 *   - mutex  : MutexOffsetTracker (kotlinx.coroutines Mutex)
 *   - actor  : Actor-like wrapper with Channel and single owner of tracker
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@State(Scope.Benchmark)
open class OffsetTrackerConcurrencyBenchmark {

    interface ThreadSafeOffsetTracker {
        suspend fun markProcessed(offset: Long)
        suspend fun advanceCommitOffset(): Long?
    }

    /**
     * Number of worker coroutines processing offsets.
     */
    @Param("300")
    var p1_workers: Int = 0

    /**
     * Total number of offsets to process per run.
     */
    @Param("50000")
    var p2_totalMessages: Int = 0

    /**
     * Implementation type:
     *  - sync
     *  - mutex
     *  - actor
     */
    @Param("final_ring_bitset", "sync_ring_bitset", "lock_ring_bitset", "mutex_ring_bitset", "actor_ring_bitset", "concurrent_hash_set")
    lateinit var p3_impl: String

    val random = Random(12345)

    @Benchmark
    fun realisticScenario(): Long = runBlocking {
        when (p3_impl) {
            "sync_ring_bitset" -> runScenario( object : ThreadSafeOffsetTracker {

                val delegate = RingBitsetOffsetTracker(-1)
                val lock = Any()

                override suspend fun markProcessed(offset: Long) {
                    synchronized(lock) {
                        delegate.markProcessed(offset)
                    }
                }

                override suspend fun advanceCommitOffset(): Long? {
                    synchronized(lock) {
                        return delegate.advanceCommitOffset()
                    }
                }
            })
            "lock_ring_bitset" -> runScenario( object : ThreadSafeOffsetTracker {

                val delegate = RingBitsetOffsetTracker(-1)
                val lock = ReentrantLock()

                override suspend fun markProcessed(offset: Long) {
                    lock.withLock { delegate.markProcessed(offset) }
                }

                override suspend fun advanceCommitOffset(): Long? {
                    lock.withLock { return delegate.advanceCommitOffset() }
                }
            })
            "final_ring_bitset" -> runScenario( object : ThreadSafeOffsetTracker {

                val delegate = OffsetTracker(-1)

                override suspend fun markProcessed(offset: Long) = delegate.markProcessed(offset)

                override suspend fun advanceCommitOffset(): Long? = delegate.advanceCommitOffset()
            })
            "mutex_ring_bitset" -> runScenario(object : ThreadSafeOffsetTracker {

                val delegate = RingBitsetOffsetTracker(-1)
                val mutex = Mutex()

                override suspend fun markProcessed(offset: Long) {
                    mutex.withLock { delegate.markProcessed(offset) }
                }

                override suspend fun advanceCommitOffset(): Long? {
                    return mutex.withLock { delegate.advanceCommitOffset() }
                }
            })
            "actor_ring_bitset" -> runScenario(object : ThreadSafeOffsetTracker {
                val delegate = RingBitsetOffsetTracker(-1)
                val channel = Channel<Long>()
                override suspend fun markProcessed(offset: Long) {
                    channel.send(offset)
                }

                override suspend fun advanceCommitOffset(): Long? {
                    while (true) {
                        val offset = channel.tryReceive().getOrNull() ?: break
                        delegate.markProcessed(offset)
                    }
                    return delegate.advanceCommitOffset()
                }
            })
            "concurrent_hash_set" -> runScenario(object : ThreadSafeOffsetTracker {
                val delegate = HashSetOffsetTracker(-1, {
                    Collections.newSetFromMap(ConcurrentHashMap())
                })
                override suspend fun markProcessed(offset: Long) {
                    delegate.markProcessed(offset)
                }

                override suspend fun advanceCommitOffset(): Long? {
                    return delegate.advanceCommitOffset()
                }
            })
            "reference" -> runScenario(object : ThreadSafeOffsetTracker {
                var i: Long = -1
                override suspend fun markProcessed(offset: Long) {
                    i = offset
                }

                override suspend fun advanceCommitOffset(): Long? {
                    return i
                }
            })
            else -> error("Unknown impl: $p3_impl")
        }
    }

    private suspend fun CoroutineScope.runScenario(tracker: ThreadSafeOffsetTracker): Long {

        val channel = Channel<Long>(Channel.UNLIMITED)

        // producer: push 1..totalMessages and close
        launch(Dispatchers.Default) {
            for (i in 1..p2_totalMessages) {
                channel.send(i.toLong())
            }
            channel.close()
        }

        // commit coroutine: every 10 ms calls advanceCommitOffset()
        val commitJob = launch(Dispatchers.Default) {
            while (isActive) {
                tracker.advanceCommitOffset()
                delay(random.nextInt(1,3).toLong())
            }
        }

        // workers
        val workerJobs = List(p1_workers) {
            launch(Dispatchers.Default) {
                for (offset in channel) {
                    tracker.markProcessed(offset)
                    if (offset and 15L == 0L)
                        delay(1L)
                }
            }
        }

        // wait for workers to finish
        workerJobs.joinAll()

        // after all processed, do final drain of commits
        var last = -1L
        while (true) {
            val c = tracker.advanceCommitOffset() ?: break
            last = c
        }

        // stop commit loop
        commitJob.cancelAndJoin()

        return last
    }
}
