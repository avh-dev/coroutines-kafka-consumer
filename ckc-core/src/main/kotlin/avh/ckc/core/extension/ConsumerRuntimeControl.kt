package avh.ckc.core.extension

import org.apache.kafka.common.TopicPartition

/** Controls a single runtime independently of any lifecycle that coordinates several runtimes. */
interface ConsumerRuntimeControl {
    fun start()
    suspend fun awaitReady()
    suspend fun stop()
    suspend fun abort(cause: Throwable)
    suspend fun awaitTermination()
    suspend fun awaitShutdownCompletion()
    fun isShutdownComplete(): Boolean

    /** Returns current ownership generations for a ready runtime, or an empty map. */
    fun partitionOwnership(topic: String): Map<TopicPartition, Long>
}

/** Coordinates the lifecycle exposed by a consumer that uses a runtime extension. */
interface ManagedConsumerLifecycle {
    fun start()
    suspend fun awaitReady()
    suspend fun stop()
    suspend fun awaitTermination()
    fun isStopComplete(): Boolean
}
