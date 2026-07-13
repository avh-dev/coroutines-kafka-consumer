package avh.ckc.demo.consumer.springkafkacoroutinesnaive

import avh.ckc.core.ProcessingMode
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.AuditDropReasons
import avh.ckc.demo.consumer.FreshnessFirstRecordFilter
import avh.ckc.demo.logFailed
import avh.ckc.demo.logDropped
import avh.ckc.demo.logProcessed
import avh.ckc.demo.logRetryAttempt
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.service.DemoConsumerRecordContext
import avh.ckc.demo.service.DemoRecordMetrics
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.kafka.listener.AbstractMessageListenerContainer

class SpringKafkaCoroutinesNaiveRuntime(
    private val properties: DemoApplicationProperties,
    private val orderConsumerMetrics: ConsumerMetrics<String, OrderLifecycleEvent>,
    private val batchConsumerMetrics: ConsumerMetrics<String, BatchLifecycleEvent>,
    private val telemetryConsumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>,
    private val workerDispatcher: CoroutineDispatcher,
    private val orderHandler: suspend (OrderLifecycleEvent) -> Unit,
    private val batchHandler: suspend (BatchLifecycleEvent) -> Unit,
    private val telemetryHandler: suspend (CauldronTelemetryEvent) -> Unit,
    private val recordMetrics: DemoRecordMetrics = DemoRecordMetrics(),
    private val freshnessFirstRecordFilter: FreshnessFirstRecordFilter = FreshnessFirstRecordFilter(properties)
) : SmartLifecycle {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val orderChannel = Channel<ConsumerRecord<String, OrderLifecycleEvent>>(properties.consumers.order.workChannelCapacity)
    private val batchChannel = Channel<ConsumerRecord<String, BatchLifecycleEvent>>(properties.consumers.batch.workChannelCapacity)
    private val telemetryChannel = Channel<ConsumerRecord<String, CauldronTelemetryEvent>>(
        properties.consumers.telemetry.workChannelCapacity
    )
    private var scope = newScope()
    private val workerJobs = mutableListOf<Job>()

    @Volatile
    private var running = false

    fun enqueueOrder(record: ConsumerRecord<String, OrderLifecycleEvent>) {
        enqueue(orderChannel, record)
    }

    fun enqueueBatch(record: ConsumerRecord<String, BatchLifecycleEvent>) {
        enqueue(batchChannel, record)
    }

    fun enqueueTelemetry(record: ConsumerRecord<String, CauldronTelemetryEvent>) {
        enqueue(telemetryChannel, record)
    }

    override fun start() {
        if (running) {
            return
        }
        scope = newScope()
        launchWorkers("order", properties.consumers.order.workerConcurrency) {
            processOrder(orderChannel.receive())
        }
        launchWorkers("batch", properties.consumers.batch.workerConcurrency) {
            processBatch(batchChannel.receive())
        }
        launchWorkers("telemetry", properties.consumers.telemetry.workerConcurrency) {
            processTelemetry(telemetryChannel.receive())
        }
        running = true
    }

    override fun stop() {
        if (!running) {
            return
        }
        scope.cancel()
        orderChannel.close()
        batchChannel.close()
        telemetryChannel.close()
        workerJobs.clear()
        running = false
    }

    override fun isRunning(): Boolean = running

    override fun isAutoStartup(): Boolean = true

    override fun getPhase(): Int = AbstractMessageListenerContainer.DEFAULT_PHASE - 1

    private fun <V> enqueue(
        channel: Channel<ConsumerRecord<String, V>>,
        record: ConsumerRecord<String, V>
    ) {
        channel.trySendBlocking(record).getOrThrow()
    }

    private fun launchWorkers(
        name: String,
        concurrency: Int,
        block: suspend () -> Unit
    ) {
        require(concurrency > 0) {
            "demo.consumers.$name.worker-concurrency must be > 0 for spring-kafka-coroutines-naive"
        }
        repeat(concurrency) { index ->
            workerJobs += scope.launch {
                while (isActive) {
                    block()
                }
            }.also { job ->
                job.invokeOnCompletion { error ->
                    if (error != null) {
                        logger.debug("Naive {} worker {} stopped", name, index + 1, error)
                    }
                }
            }
        }
    }

    private suspend fun processOrder(record: ConsumerRecord<String, OrderLifecycleEvent>) {
        val event = record.value() ?: return
        processRecord(
            record = record,
            value = event,
            runtime = properties.consumers.order,
            metrics = orderConsumerMetrics,
            handle = { orderHandler(event) }
        )
    }

    private suspend fun processBatch(record: ConsumerRecord<String, BatchLifecycleEvent>) {
        val event = record.value() ?: return
        processRecord(
            record = record,
            value = event,
            runtime = properties.consumers.batch,
            metrics = batchConsumerMetrics,
            handle = { batchHandler(event) }
        )
    }

    private suspend fun processTelemetry(record: ConsumerRecord<String, CauldronTelemetryEvent>) {
        val event = record.value() ?: return
        processRecord(
            record = record,
            value = event,
            runtime = properties.consumers.telemetry,
            metrics = telemetryConsumerMetrics,
            handle = { telemetryHandler(event) }
        )
    }

    private suspend fun <V> processRecord(
        record: ConsumerRecord<String, V>,
        value: V,
        runtime: DemoApplicationProperties.ConsumerRuntime,
        metrics: ConsumerMetrics<String, V>,
        handle: suspend () -> Unit
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
                    latencyOnlyDelay()
                }
                recordMetrics.onProcessed(metrics, context, value, startedAt)
                logProcessed(record, properties.audit)
                return
            } catch (error: Throwable) {
                recordMetrics.onFailed(metrics, context, value, startedAt, error)
                if (attempt >= properties.consumers.retry.maxAttempts) {
                    logFailed(record, properties.audit)
                    logger.warn(
                        "Naive Spring Kafka coroutine worker final failure topic={}, partition={}, offset={} after {} attempts",
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
                    "Naive Spring Kafka coroutine worker retrying record topic={}, partition={}, offset={} after attempt {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    attempt,
                    error
                )
                attempt += 1
                delay(properties.consumers.retry.backoffMs)
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
                    "Discarding stale naive Spring Kafka coroutine record for topic={}, offset={}",
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

    private suspend fun latencyOnlyDelay() {
        delay((5L..8L).random())
    }

    private fun newScope(): CoroutineScope =
        CoroutineScope(SupervisorJob() + workerDispatcher)
}

internal fun ProcessingMode.requireSupportedBySpringKafkaCoroutinesNaive(): ProcessingMode =
    when (this) {
        ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
        ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST -> this
        ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING,
        ProcessingMode.AT_LEAST_ONCE_PARTITION_ORDERING,
        ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY ->
            throw IllegalArgumentException(
                "Processing mode $this is not supported by the spring-kafka-coroutines-naive demo profile"
            )
    }
