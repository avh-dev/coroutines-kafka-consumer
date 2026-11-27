package avh.ckc.core.extension

import org.apache.kafka.common.TopicPartition

/**
 * Controls one runtime directly, bypassing any attached [ManagedConsumerLifecycle]. Use this
 * interface for a runtime owned by your extension. The extension is responsible for stopping
 * every runtime it creates and waiting for shutdown when releasing resources shared with it.
 */
interface ConsumerRuntimeControl {
    /** Starts the runtime once. */
    fun start()

    /** Waits for every poll loop's first assignment and initial positions, or throws its failure. */
    suspend fun awaitReady()

    /** Requests graceful cleanup and waits for it. A cancelled caller can resume waiting later. */
    suspend fun stop()

    /** Forces termination with [cause] and waits for the shutdown sequence. May rethrow the terminal failure. */
    suspend fun abort(cause: Throwable)

    /** Waits for normal termination or rethrows the terminal failure, which can precede shutdown completion. */
    suspend fun awaitTermination()

    /** Waits for the runtime's shutdown sequence, including resource release attempts. */
    suspend fun awaitShutdownCompletion()

    /** Reports whether the runtime's shutdown sequence has finished. */
    fun isShutdownComplete(): Boolean

    /**
     * Returns current ownership generations for [topic] once the runtime is ready, or an empty
     * map before readiness, after stop, or on failure. Each generation identifies one lease.
     */
    fun partitionOwnership(topic: String): Map<TopicPartition, Long>
}

/**
 * Coordinates the public consumer lifecycle when an extension owns several runtimes.
 * Install it before start and use [ConsumerRuntimeControl] to operate on the underlying runtime.
 */
interface ManagedConsumerLifecycle {
    /** Requests startup. Readiness can be published asynchronously. */
    fun start()

    /** Waits for the extension's readiness condition or reports startup failure. */
    suspend fun awaitReady()

    /** Stops this consumer and its extension-owned work. Keep unfinished cleanup available for retry. */
    suspend fun stop()

    /** Waits for normal termination or reports terminal failure. Resource cleanup can continue afterward. */
    suspend fun awaitTermination()

    /** Reports whether both the runtime and resources owned by this lifecycle have finished cleanup. */
    fun isStopComplete(): Boolean
}
