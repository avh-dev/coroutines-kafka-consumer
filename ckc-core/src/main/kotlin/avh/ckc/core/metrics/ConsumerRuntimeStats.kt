package avh.ckc.core.metrics

/**
 * Read-only view of a running consumer's worker and queue state.
 *
 * Implementations are owned by the consumer runtime. Metrics adapters receive this interface when runtime gauges
 * are bound and should read values from it without assuming how frequently they change or how they are updated.
 */
interface ConsumerRuntimeStats {
    /** Configured number of worker coroutines. */
    val workerCount: Int

    /** Number of worker coroutines currently processing records. */
    val activeWorkerCount: Int

    /** Current number of records queued between poll loops and workers. */
    val workQueueSize: Int

    /** Configured capacity of the work queue. */
    val workQueueCapacity: Int

    /** Largest work queue size observed since the consumer started. */
    val maxObservedWorkQueueSize: Int

    /** Current number of records waiting behind an in-flight ordering key or partition. */
    val orderingQueueSize: Int

    /** Largest ordering queue size observed since the consumer started. */
    val maxObservedOrderingQueueSize: Int
}
