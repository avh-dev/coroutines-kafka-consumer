package avh.ckc.demo.consumer.springkafkathreadpool

import avh.ckc.core.ProcessingMode
import avh.ckc.core.metrics.ConsumerMetrics
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
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class SpringKafkaThreadPoolRuntime(
    private val properties: DemoApplicationProperties,
    private val orderConsumerMetrics: ConsumerMetrics<String, OrderLifecycleEvent>,
    private val batchConsumerMetrics: ConsumerMetrics<String, BatchLifecycleEvent>,
    private val telemetryConsumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>,
    private val orderHandler: (OrderLifecycleEvent) -> Unit,
    private val batchHandler: (BatchLifecycleEvent) -> Unit,
    private val telemetryHandler: (CauldronTelemetryEvent) -> Unit,
    private val recordMetrics: DemoRecordMetrics = DemoRecordMetrics(),
    private val freshnessFirstRecordFilter: FreshnessFirstRecordFilter = FreshnessFirstRecordFilter(properties)
) : SmartLifecycle {
    private val logger = LoggerFactory.getLogger(javaClass)
    private lateinit var orderExecutor: ThreadPoolExecutor
    private lateinit var batchExecutor: ThreadPoolExecutor
    private lateinit var telemetryExecutor: ThreadPoolExecutor

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
        orderExecutor = newExecutor("order", properties.consumers.order)
        batchExecutor = newExecutor("batch", properties.consumers.batch)
        telemetryExecutor = newExecutor("telemetry", properties.consumers.telemetry)
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

    private fun newExecutor(name: String, runtime: DemoApplicationProperties.ConsumerRuntime): ThreadPoolExecutor {
        require(runtime.workerConcurrency > 0) {
            "demo.consumers.$name.worker-concurrency must be > 0 for spring-kafka-thread-pool"
        }
        require(runtime.workChannelCapacity > 0) {
            "demo.consumers.$name.work-channel-capacity must be > 0 for spring-kafka-thread-pool"
        }
        val threadNumber = AtomicInteger()
        return ThreadPoolExecutor(
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

    private fun <V> enqueue(
        executor: ThreadPoolExecutor,
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
