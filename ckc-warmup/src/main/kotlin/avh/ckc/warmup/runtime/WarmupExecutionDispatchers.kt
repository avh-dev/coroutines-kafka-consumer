package avh.ckc.warmup.runtime

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeoutOrNull

/**
 * Owns the context-wide protocol executors.
 *
 * Coordination is isolated from blocking Kafka calls so a stalled client cannot consume the thread
 * needed to enforce deadlines or initiate shutdown. The blocking pool is shared by every mapping
 * and session in the context; the long-lived control poll may retain one of its workers.
 */
internal class WarmupExecutionDispatchers(
    contextName: String,
    maxThreads: Int,
    idleTimeout: Duration
) {
    private val validatedMaxThreads = requireThreadLimit(maxThreads)
    private val validatedIdleTimeout = requireIdleTimeout(idleTimeout)
    private val threadSequence = AtomicInteger()
    private val threadFactory = ThreadFactory { task ->
        Thread(task, "ckc-warmup-${sanitizeThreadName(contextName)}-${threadSequence.incrementAndGet()}").apply {
            isDaemon = true
        }
    }
    private val coordinationExecutor = expiringExecutor(1, validatedIdleTimeout)
    private val blockingExecutor = expiringExecutor(validatedMaxThreads - 1, validatedIdleTimeout)

    val coordination: ExecutorCoroutineDispatcher = coordinationExecutor.asCoroutineDispatcher()
    val blocking: ExecutorCoroutineDispatcher = blockingExecutor.asCoroutineDispatcher()

    /** Starts shutdown once and permits subsequent calls to keep waiting for termination. */
    suspend fun close(timeout: Duration): Boolean {
        coordination.close()
        blocking.close()
        return withTimeoutOrNull(timeout) {
            while (!coordinationExecutor.isTerminated || !blockingExecutor.isTerminated) delay(10)
            true
        } == true
    }

    internal fun liveThreadCount(): Int = coordinationExecutor.poolSize + blockingExecutor.poolSize

    private fun expiringExecutor(threadCount: Int, idleTimeout: Duration) = ThreadPoolExecutor(
        threadCount,
        threadCount,
        idleTimeout.inWholeMilliseconds,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(),
        threadFactory
    ).apply {
        allowCoreThreadTimeOut(true)
    }

    private companion object {
        const val MIN_THREAD_LIMIT = 3

        fun requireThreadLimit(value: Int): Int = value.also {
            require(it >= MIN_THREAD_LIMIT) {
                "Warm-up service thread limit must be at least $MIN_THREAD_LIMIT"
            }
        }

        fun requireIdleTimeout(value: Duration): Duration = value.also {
            require(it.isPositive() && it.inWholeMilliseconds > 0) {
                "Warm-up service thread idle timeout must be at least one millisecond"
            }
        }

        fun sanitizeThreadName(value: String): String = value
            .replace(Regex("[^A-Za-z0-9_-]+"), "-")
            .trim('-')
            .ifBlank { "context" }
            .take(32)
    }
}
