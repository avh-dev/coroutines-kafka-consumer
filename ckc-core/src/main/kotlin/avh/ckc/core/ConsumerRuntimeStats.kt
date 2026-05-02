package avh.ckc.core

import java.util.concurrent.atomic.AtomicInteger

interface ConsumerRuntimeStats {
    val workerCount: Int
    val activeWorkerCount: Int
    val workQueueSize: Int
    val workQueueCapacity: Int
    val maxObservedWorkQueueSize: Int
}

internal class DefaultConsumerRuntimeStats(
    override val workerCount: Int,
    override val workQueueCapacity: Int
) : ConsumerRuntimeStats {
    private val activeWorkerCountRef = AtomicInteger(0)
    private val workQueueSizeRef = AtomicInteger(0)
    private val maxObservedWorkQueueSizeRef = AtomicInteger(0)

    override val activeWorkerCount: Int
        get() = activeWorkerCountRef.get()

    override val workQueueSize: Int
        get() = workQueueSizeRef.get()

    override val maxObservedWorkQueueSize: Int
        get() = maxObservedWorkQueueSizeRef.get()

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

    fun onWorkDequeued() {
        workQueueSizeRef.updateAndGet { current ->
            if (current > 0) current - 1 else 0
        }
    }

    fun onWorkerStarted() {
        activeWorkerCountRef.incrementAndGet()
    }

    fun onWorkerFinished() {
        activeWorkerCountRef.updateAndGet { current ->
            if (current > 0) current - 1 else 0
        }
    }
}
