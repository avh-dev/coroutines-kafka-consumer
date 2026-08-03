package avh.ckc.demo.consumer.springkafkathreadpool

import avh.ckc.core.ProcessingMode
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.ConsumerRuntimeStats
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.demo.AuditDropReasons
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.consumer.FreshnessFirstRecordFilter
import avh.ckc.demo.logDropped
import avh.ckc.demo.logFailed
import avh.ckc.demo.logProcessed
import avh.ckc.demo.logRetryAttempt
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.service.DemoConsumerRecordContext
import avh.ckc.demo.service.DemoRecordMetrics
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.kafka.listener.AbstractMessageListenerContainer
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

enum class SpringKafkaThreadPoolExecutorMode {
    PLATFORM_THREAD_POOL,
    VIRTUAL_THREAD_PER_TASK
}

class SpringKafkaThreadPoolRuntime(
    private val properties: DemoApplicationProperties,
    private val orderConsumerMetrics: ConsumerMetrics<String, OrderLifecycleEvent>,
    private val batchConsumerMetrics: ConsumerMetrics<String, BatchLifecycleEvent>,
    private val telemetryConsumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>,
    private val orderHandler: (OrderLifecycleEvent) -> Unit,
    private val batchHandler: (BatchLifecycleEvent) -> Unit,
    private val telemetryHandler: (CauldronTelemetryEvent) -> Unit,
    private val executorMode: SpringKafkaThreadPoolExecutorMode = SpringKafkaThreadPoolExecutorMode.PLATFORM_THREAD_POOL,
    private val recordMetrics: DemoRecordMetrics = DemoRecordMetrics(),
    private val freshnessFirstRecordFilter: FreshnessFirstRecordFilter = FreshnessFirstRecordFilter(properties)
) : SmartLifecycle {
    private val logger = LoggerFactory.getLogger(javaClass)
    private lateinit var orderExecutor: SpringKafkaWorkerExecutor
    private lateinit var batchExecutor: SpringKafkaWorkerExecutor
    private lateinit var telemetryExecutor: SpringKafkaWorkerExecutor

    @Volatile
    private var running = false

    fun enqueueOrder(record: ConsumerRecord<String, OrderLifecycleEvent>) {
        enqueue(orderExecutor, record, orderConsumerMetrics) {
            processOrder(record)
        }
    }

    fun enqueueBatch(record: ConsumerRecord<String, BatchLifecycleEvent>) {
        enqueue(batchExecutor, record, batchConsumerMetrics) {
            processBatch(record)
        }
    }

    fun enqueueTelemetry(record: ConsumerRecord<String, CauldronTelemetryEvent>) {
        enqueue(telemetryExecutor, record, telemetryConsumerMetrics) {
            processTelemetry(record)
        }
    }

    override fun start() {
        if (running) {
            return
        }
        orderExecutor = newExecutor("order", properties.consumers.order, orderConsumerMetrics)
        batchExecutor = newExecutor("batch", properties.consumers.batch, batchConsumerMetrics)
        telemetryExecutor = newExecutor("telemetry", properties.consumers.telemetry, telemetryConsumerMetrics)
        running = true
    }

    override fun stop() {
        if (!running) {
            return
        }
        orderExecutor.shutdownNow()
        batchExecutor.shutdownNow()
        telemetryExecutor.shutdownNow()
        running = false
    }

    override fun isRunning(): Boolean = running

    override fun isAutoStartup(): Boolean = true

    override fun getPhase(): Int = AbstractMessageListenerContainer.DEFAULT_PHASE - 1

    private fun newExecutor(
        name: String,
        runtime: DemoApplicationProperties.ConsumerRuntime,
        metrics: ConsumerMetrics<String, *>
    ): SpringKafkaWorkerExecutor {
        require(runtime.workerConcurrency > 0) {
            "demo.consumers.$name.worker-concurrency must be > 0 for spring-kafka-thread-pool"
        }
        require(runtime.workChannelCapacity > 0) {
            "demo.consumers.$name.work-channel-capacity must be > 0 for spring-kafka-thread-pool"
        }
        val stats = SpringKafkaWorkerRuntimeStats(runtime.workerConcurrency, runtime.workChannelCapacity)
        metrics.bindRuntimeMetrics(stats)
        return when (executorMode) {
            SpringKafkaThreadPoolExecutorMode.PLATFORM_THREAD_POOL ->
                PlatformSpringKafkaWorkerExecutor(name, runtime, stats)
            SpringKafkaThreadPoolExecutorMode.VIRTUAL_THREAD_PER_TASK ->
                VirtualThreadSpringKafkaWorkerExecutor(name, runtime, stats)
        }
    }

    private fun <V> enqueue(
        executor: SpringKafkaWorkerExecutor,
        record: ConsumerRecord<String, V>,
        metrics: ConsumerMetrics<String, V>,
        task: () -> Unit
    ) {
        val value = record.value() ?: return
        try {
            executor.execute(task)
        } catch (_: RejectedExecutionException) {
            recordMetrics.onDropped(metrics, record.context(), value, RecordDropReason.QUEUE_OVERFLOW)
            logDropped(record, properties.audit, AuditDropReasons.ADMISSION_FAILED)
            logger.warn(
                "Spring Kafka thread-pool admission dropped record topic={}, partition={}, offset={} because the worker queue is full",
                record.topic(),
                record.partition(),
                record.offset()
            )
        }
    }

    private fun processOrder(record: ConsumerRecord<String, OrderLifecycleEvent>) {
        val event = record.value() ?: return
        processRecord(
            record = record,
            value = event,
            runtime = properties.consumers.order,
            metrics = orderConsumerMetrics,
            handle = { orderHandler(event) }
        )
    }

    private fun processBatch(record: ConsumerRecord<String, BatchLifecycleEvent>) {
        val event = record.value() ?: return
        processRecord(
            record = record,
            value = event,
            runtime = properties.consumers.batch,
            metrics = batchConsumerMetrics,
            handle = { batchHandler(event) }
        )
    }

    private fun processTelemetry(record: ConsumerRecord<String, CauldronTelemetryEvent>) {
        val event = record.value() ?: return
        processRecord(
            record = record,
            value = event,
            runtime = properties.consumers.telemetry,
            metrics = telemetryConsumerMetrics,
            handle = { telemetryHandler(event) }
        )
    }

    private fun <V> processRecord(
        record: ConsumerRecord<String, V>,
        value: V,
        runtime: DemoApplicationProperties.ConsumerRuntime,
        metrics: ConsumerMetrics<String, V>,
        handle: () -> Unit
    ) {
        val context = record.context()
        val startedAt = System.nanoTime()
        if (shouldDiscard(runtime, context)) {
            recordMetrics.onDropped(metrics, context, value, RecordDropReason.STALE_AGE)
            logDropped(record, properties.audit, AuditDropReasons.STALE_AGE)
            return
        }

        var attempt = 1
        while (true) {
            try {
                if (properties.consumers.processingEnabled) {
                    handle()
                } else {
                    latencyOnlySleep()
                }
                recordMetrics.onProcessed(metrics, context, value, startedAt)
                logProcessed(record, properties.audit)
                return
            } catch (error: Throwable) {
                recordMetrics.onFailed(metrics, context, value, startedAt, error)
                if (attempt >= properties.consumers.retry.maxAttempts) {
                    logFailed(record, properties.audit)
                    logger.warn(
                        "Spring Kafka thread-pool worker final failure topic={}, partition={}, offset={} after {} attempts",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        attempt,
                        error
                    )
                    return
                }

                recordMetrics.onRetry(metrics, context, value, attempt, error)
                logRetryAttempt(record, properties.audit)
                logger.warn(
                    "Spring Kafka thread-pool worker retrying record topic={}, partition={}, offset={} after attempt {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    attempt,
                    error
                )
                attempt += 1
                Thread.sleep(properties.consumers.retry.backoffMs)
            }
        }
    }

    private fun shouldDiscard(
        runtime: DemoApplicationProperties.ConsumerRuntime,
        context: DemoConsumerRecordContext
    ): Boolean =
        freshnessFirstRecordFilter.shouldDiscard(runtime, context.timestamp).also { discard ->
            if (discard) {
                logger.debug(
                    "Discarding stale Spring Kafka thread-pool record for topic={}, offset={}",
                    context.topic,
                    context.offset
                )
            }
        }

    private fun <V> ConsumerRecord<String, V>.context(): DemoConsumerRecordContext =
        DemoConsumerRecordContext(
            key = key(),
            topic = topic(),
            partition = partition(),
            offset = offset(),
            timestamp = timestamp()
        )

    private fun latencyOnlySleep() {
        Thread.sleep((5L..8L).random())
    }
}

private interface SpringKafkaWorkerExecutor {
    fun execute(task: () -> Unit)

    fun shutdownNow()
}

private class PlatformSpringKafkaWorkerExecutor(
    name: String,
    runtime: DemoApplicationProperties.ConsumerRuntime,
    private val stats: SpringKafkaWorkerRuntimeStats
) : SpringKafkaWorkerExecutor {
    private val executor: ThreadPoolExecutor

    init {
        val threadNumber = AtomicInteger()
        executor = ThreadPoolExecutor(
            runtime.workerConcurrency,
            runtime.workerConcurrency,
            0L,
            TimeUnit.MILLISECONDS,
            ArrayBlockingQueue(runtime.workChannelCapacity),
            { runnable ->
                Thread(runnable, "spring-kafka-thread-pool-$name-${threadNumber.incrementAndGet()}").apply {
                    isDaemon = true
                }
            },
            ThreadPoolExecutor.AbortPolicy()
        )
    }

    override fun execute(task: () -> Unit) {
        stats.onWorkEnqueued()
        try {
            executor.execute {
                stats.onWorkDequeued()
                stats.onWorkerStarted()
                try {
                    task()
                } finally {
                    stats.onWorkerFinished()
                }
            }
        } catch (error: RejectedExecutionException) {
            stats.onWorkDequeued()
            throw error
        }
    }

    override fun shutdownNow() {
        executor.shutdownNow()
    }
}

private class VirtualThreadSpringKafkaWorkerExecutor(
    name: String,
    runtime: DemoApplicationProperties.ConsumerRuntime,
    private val stats: SpringKafkaWorkerRuntimeStats
) : SpringKafkaWorkerExecutor {
    private val workerPermits = Semaphore(runtime.workerConcurrency)
    private val admissionPermits = Semaphore(runtime.workerConcurrency + runtime.workChannelCapacity)
    private val executor: ExecutorService

    init {
        val threadNumber = AtomicInteger()
        executor = Executors.newThreadPerTaskExecutor { runnable ->
            Thread.ofVirtual()
                .name("spring-kafka-virtual-thread-pool-$name-", threadNumber.incrementAndGet().toLong())
                .factory()
                .newThread(runnable)
        }
    }

    override fun execute(task: () -> Unit) {
        if (!admissionPermits.tryAcquire()) {
            throw RejectedExecutionException("virtual-thread worker admission is full")
        }
        stats.onWorkEnqueued()
        try {
            executor.execute {
                var workerAcquired = false
                var workDequeued = false
                try {
                    workerPermits.acquire()
                    workerAcquired = true
                    stats.onWorkDequeued()
                    workDequeued = true
                    stats.onWorkerStarted()
                    try {
                        task()
                    } finally {
                        stats.onWorkerFinished()
                    }
                } catch (_: InterruptedException) {
                    Thread.currentThread().interrupt()
                } finally {
                    if (!workDequeued) {
                        stats.onWorkDequeued()
                    }
                    if (workerAcquired) {
                        workerPermits.release()
                    }
                    admissionPermits.release()
                }
            }
        } catch (error: RejectedExecutionException) {
            stats.onWorkDequeued()
            admissionPermits.release()
            throw error
        }
    }

    override fun shutdownNow() {
        executor.shutdownNow()
    }
}

private class SpringKafkaWorkerRuntimeStats(
    override val workerCount: Int,
    override val workQueueCapacity: Int
) : ConsumerRuntimeStats {
    private val activeWorkerCountRef = AtomicInteger()
    private val workQueueSizeRef = AtomicInteger()
    private val maxObservedWorkQueueSizeRef = AtomicInteger()

    override val activeWorkerCount: Int
        get() = activeWorkerCountRef.get()

    override val workQueueSize: Int
        get() = workQueueSizeRef.get()

    override val maxObservedWorkQueueSize: Int
        get() = maxObservedWorkQueueSizeRef.get()

    override val orderingQueueSize: Int
        get() = 0

    override val maxObservedOrderingQueueSize: Int
        get() = 0

    fun onWorkEnqueued() {
        val queueSize = workQueueSizeRef.incrementAndGet()
        maxObservedWorkQueueSizeRef.updateAndGet { current -> maxOf(current, queueSize) }
    }

    fun onWorkDequeued() {
        workQueueSizeRef.updateAndGet { current -> if (current > 0) current - 1 else 0 }
    }

    fun onWorkerStarted() {
        activeWorkerCountRef.incrementAndGet()
    }

    fun onWorkerFinished() {
        activeWorkerCountRef.updateAndGet { current -> if (current > 0) current - 1 else 0 }
    }
}

internal fun ProcessingMode.requireSupportedBySpringKafkaThreadPool(): ProcessingMode =
    when (this) {
        ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
        ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST -> this
        ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING,
        ProcessingMode.AT_LEAST_ONCE_PARTITION_ORDERING,
        ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY ->
            throw IllegalArgumentException(
                "Processing mode $this is not supported by the spring-kafka-thread-pool demo profile"
            )
    }
