package avh.ckc.demo.consumer.confluent

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
import avh.ckc.demo.service.batch.SuspendBatchLifecycleService
import avh.ckc.demo.service.cauldron.SuspendCauldronTelemetryService
import avh.ckc.demo.service.order.SuspendOrderLifecycleService
import io.confluent.parallelconsumer.PCRetriableException
import io.confluent.parallelconsumer.RecordContext
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.reactor.mono
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
@Profile("confluent-parallel-reactor")
class ConfluentParallelReactorTrackingService(
    private val properties: DemoApplicationProperties,
    private val orderLifecycleService: SuspendOrderLifecycleService,
    private val batchLifecycleService: SuspendBatchLifecycleService,
    private val cauldronTelemetryService: SuspendCauldronTelemetryService,
    private val recordMetrics: DemoRecordMetrics,
    private val freshnessFirstRecordFilter: FreshnessFirstRecordFilter,
    @Qualifier("confluentParallelOrderConsumerMetrics")
    private val orderConsumerMetrics: ConsumerMetrics<String, OrderLifecycleEvent>,
    @Qualifier("confluentParallelBatchConsumerMetrics")
    private val batchConsumerMetrics: ConsumerMetrics<String, BatchLifecycleEvent>,
    @Qualifier("confluentParallelConsumerMetrics")
    private val telemetryConsumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>,
    @Qualifier("confluentParallelReactorWorkerDispatcher")
    private val workerDispatcher: CoroutineDispatcher
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun processOrderLifecycle(context: RecordContext<String, OrderLifecycleEvent>): Mono<Boolean> =
        mono(context = workerDispatcher) {
            val record = context.consumerRecord
            val startedAt = System.nanoTime()
            try {
                if (shouldDiscard(properties.consumers.order, record)) {
                    recordMetrics.onDropped(
                        orderConsumerMetrics,
                        record.context(),
                        record.value(),
                        RecordDropReason.STALE_AGE
                    )
                    logDropped(record, properties.audit, AuditDropReasons.STALE_AGE)
                    return@mono processingCompleted()
                }
                if (properties.consumers.processingEnabled) {
                    orderLifecycleService.apply(record.value())
                } else {
                    latencyOnlyDelay()
                }
                recordMetrics.onProcessed(orderConsumerMetrics, record.context(), record.value(), startedAt)
                logProcessed(record, properties.audit)
                logger.debug("Confluent Parallel Reactor order event received for key={}, order={}", record.key(), record.value().orderId)
                processingCompleted()
            } catch (error: Throwable) {
                handleFailure(context, orderConsumerMetrics, startedAt, error)
            }
        }

    fun processBatchLifecycle(context: RecordContext<String, BatchLifecycleEvent>): Mono<Boolean> =
        mono(context = workerDispatcher) {
            val record = context.consumerRecord
            val startedAt = System.nanoTime()
            try {
                if (shouldDiscard(properties.consumers.batch, record)) {
                    recordMetrics.onDropped(
                        batchConsumerMetrics,
                        record.context(),
                        record.value(),
                        RecordDropReason.STALE_AGE
                    )
                    logDropped(record, properties.audit, AuditDropReasons.STALE_AGE)
                    return@mono processingCompleted()
                }
                if (properties.consumers.processingEnabled) {
                    batchLifecycleService.apply(record.value())
                } else {
                    latencyOnlyDelay()
                }
                recordMetrics.onProcessed(batchConsumerMetrics, record.context(), record.value(), startedAt)
                logProcessed(record, properties.audit)
                logger.debug("Confluent Parallel Reactor batch event received for key={}, batch={}", record.key(), record.value().batchId)
                processingCompleted()
            } catch (error: Throwable) {
                handleFailure(context, batchConsumerMetrics, startedAt, error)
            }
        }

    fun processCauldronTelemetry(context: RecordContext<String, CauldronTelemetryEvent>): Mono<Boolean> =
        mono(context = workerDispatcher) {
            val record = context.consumerRecord
            val startedAt = System.nanoTime()
            try {
                if (shouldDiscard(properties.consumers.telemetry, record)) {
                    recordMetrics.onDropped(
                        telemetryConsumerMetrics,
                        record.context(),
                        record.value(),
                        RecordDropReason.STALE_AGE
                    )
                    logDropped(record, properties.audit, AuditDropReasons.STALE_AGE)
                    return@mono processingCompleted()
                }
                if (properties.consumers.processingEnabled) {
                    cauldronTelemetryService.recalculate(record.value())
                } else {
                    latencyOnlyDelay()
                }
                recordMetrics.onProcessed(telemetryConsumerMetrics, record.context(), record.value(), startedAt)
                logProcessed(record, properties.audit)
                logger.debug(
                    "Confluent Parallel Reactor cauldron event received for key={}, cauldron={}",
                    record.key(),
                    record.value().cauldronId
                )
                processingCompleted()
            } catch (error: Throwable) {
                handleFailure(context, telemetryConsumerMetrics, startedAt, error)
            }
        }

    private suspend fun latencyOnlyDelay() {
        delay((5L..8L).random())
    }

    // ReactorProcessor 0.5.3.3 acknowledges successful work on onNext, so the publisher must emit one value.
    private fun processingCompleted(): Boolean = true

    private fun <V> handleFailure(
        context: RecordContext<String, V>,
        metrics: ConsumerMetrics<String, V>,
        startedAt: Long,
        error: Throwable
    ): Boolean {
        val record = context.consumerRecord
        val attempt = context.numberOfFailedAttempts + 1
        if (attempt >= properties.consumers.retry.maxAttempts) {
            recordMetrics.onFailed(metrics, record.context(), record.value(), startedAt, error)
            logFailed(record, properties.audit)
            logger.warn(
                "Confluent Parallel Reactor final failure for record topic={}, partition={}, offset={} after {} attempts",
                record.topic(),
                record.partition(),
                record.offset(),
                attempt,
                error
            )
            return processingCompleted()
        }

        recordMetrics.onRetry(metrics, record.context(), record.value(), attempt, error)
        logRetryAttempt(record, properties.audit)
        throw PCRetriableException(error)
    }

    private fun shouldDiscard(
        runtime: DemoApplicationProperties.ConsumerRuntime,
        record: ConsumerRecord<*, *>
    ): Boolean =
        freshnessFirstRecordFilter.shouldDiscard(runtime, record.timestamp()).also { discard ->
            if (discard) {
                logger.debug(
                    "Discarding stale Confluent Parallel Reactor record for topic={}, offset={}",
                    record.topic(),
                    record.offset()
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
}
