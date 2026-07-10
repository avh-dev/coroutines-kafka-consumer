package avh.ckc.core.metrics

import avh.ckc.core.ProcessingRuntimeStateSnapshot
import java.util.concurrent.atomic.AtomicInteger

/**
 * Tracks mutable runtime counters and exposes them through [ConsumerRuntimeStats].
 *
 * This object is updated by the consumer runtime on enqueue/dequeue and worker lifecycle transitions.
 * Metrics backends see only the read-only [ConsumerRuntimeStats] interface and sample values when their gauges
 * are scraped.
 *
 * The counters are lock-free because updates happen on hot paths:
 * - poll loops update queue size when records are dispatched;
 * - worker coroutines update active worker count around record processing;
 * - metrics readers may sample concurrently from arbitrary threads.
 */
internal class ConsumerRuntimeStatsTracker(
    override val workerCount: Int,
    override val workQueueCapacity: Int
) : ConsumerRuntimeStats {
    /** Workers that are currently executing user record processing. */
    private val activeWorkerCountRef = AtomicInteger(0)

    /** Records currently accepted by the work channel but not yet taken by a worker. */
    private val workQueueSizeRef = AtomicInteger(0)

    /** High-water mark for [workQueueSizeRef]. */
    private val maxObservedWorkQueueSizeRef = AtomicInteger(0)

    /** Records waiting behind an in-flight ordering key or partition. */
    private val orderingQueueSizeRef = AtomicInteger(0)

    /** High-water mark for [orderingQueueSizeRef]. */
    private val maxObservedOrderingQueueSizeRef = AtomicInteger(0)

    override val activeWorkerCount: Int
        get() = activeWorkerCountRef.get()

    override val workQueueSize: Int
        get() = workQueueSizeRef.get()

    override val maxObservedWorkQueueSize: Int
        get() = maxObservedWorkQueueSizeRef.get()

    override val orderingQueueSize: Int
        get() = orderingQueueSizeRef.get()

    override val maxObservedOrderingQueueSize: Int
        get() = maxObservedOrderingQueueSizeRef.get()

    /**
     * Records successful enqueue into the worker queue and updates the high-water mark.
     *
     * The max update uses CAS because multiple poll loops may enqueue concurrently.
     */
    fun onWorkEnqueued() {
        val queueSize = workQueueSizeRef.incrementAndGet()
        while (true) {
            val currentMax = maxObservedWorkQueueSizeRef.get()
            if (queueSize <= currentMax) {
                return
            }
            if (maxObservedWorkQueueSizeRef.compareAndSet(currentMax, queueSize)) {
                return
            }
        }
    }

    /**
     * Records dequeue or undelivered-element cleanup.
     *
     * The value is clamped to zero because channel cancellation can race with worker dequeue accounting.
     */
    fun onWorkDequeued() {
        workQueueSizeRef.updateAndGet { current ->
            if (current > 0) current - 1 else 0
        }
    }

    /** Records that accepted ordered work is waiting behind an in-flight record. */
    fun onOrderingWorkQueued() {
        val queueSize = orderingQueueSizeRef.incrementAndGet()
        while (true) {
            val currentMax = maxObservedOrderingQueueSizeRef.get()
            if (queueSize <= currentMax) {
                return
            }
            if (maxObservedOrderingQueueSizeRef.compareAndSet(currentMax, queueSize)) {
                return
            }
        }
    }

    /** Records dispatch or cancellation cleanup of work previously waiting on ordering. */
    fun onOrderingWorkDequeued() {
        orderingQueueSizeRef.updateAndGet { current ->
            if (current > 0) current - 1 else 0
        }
    }

    /** Records that a worker started processing a record. */
    fun onWorkerStarted() {
        activeWorkerCountRef.incrementAndGet()
    }

    /**
     * Records that a worker finished processing a record.
     *
     * The value is clamped to zero so cancellation cleanup cannot expose a negative gauge.
     */
    fun onWorkerFinished() {
        activeWorkerCountRef.updateAndGet { current ->
            if (current > 0) current - 1 else 0
        }
    }

    fun snapshot(): ProcessingRuntimeStateSnapshot =
        ProcessingRuntimeStateSnapshot(
            workerCount = workerCount,
            activeWorkerCount = activeWorkerCount,
            workQueueSize = workQueueSize,
            workQueueCapacity = workQueueCapacity,
            maxObservedWorkQueueSize = maxObservedWorkQueueSize,
            orderingQueueSize = orderingQueueSize,
            maxObservedOrderingQueueSize = maxObservedOrderingQueueSize
        )
}
