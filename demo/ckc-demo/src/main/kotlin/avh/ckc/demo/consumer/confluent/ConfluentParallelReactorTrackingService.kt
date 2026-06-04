package avh.ckc.demo.consumer.confluent

import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.consumer.FreshnessFirstRecordFilter
import avh.ckc.demo.logFailed
import avh.ckc.demo.logProcessed
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.service.batch.SuspendBatchLifecycleService
import avh.ckc.demo.service.cauldron.SuspendCauldronTelemetryService
import avh.ckc.demo.service.order.SuspendOrderLifecycleService
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
    private val freshnessFirstRecordFilter: FreshnessFirstRecordFilter,
    @Qualifier("confluentParallelReactorWorkerDispatcher")
    private val workerDispatcher: CoroutineDispatcher
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun processOrderLifecycle(record: ConsumerRecord<String, OrderLifecycleEvent>): Mono<Boolean> =
        mono(context = workerDispatcher) {
            try {
                if (shouldDiscard(properties.consumers.order, record)) return@mono processingCompleted()
                if (properties.consumers.processingEnabled) {
                    orderLifecycleService.apply(record.value())
                } else {
                    latencyOnlyDelay()
                }
                logProcessed(record, properties.audit)
                logger.debug("Confluent Parallel Reactor order event received for key={}, order={}", record.key(), record.value().orderId)
                processingCompleted()
            } catch (error: Throwable) {
                logFailed(record, properties.audit)
                throw error
            }
        }

    fun processBatchLifecycle(record: ConsumerRecord<String, BatchLifecycleEvent>): Mono<Boolean> =
        mono(context = workerDispatcher) {
            try {
                if (shouldDiscard(properties.consumers.batch, record)) return@mono processingCompleted()
                if (properties.consumers.processingEnabled) {
                    batchLifecycleService.apply(record.value())
                } else {
                    latencyOnlyDelay()
                }
                logProcessed(record, properties.audit)
                logger.debug("Confluent Parallel Reactor batch event received for key={}, batch={}", record.key(), record.value().batchId)
                processingCompleted()
            } catch (error: Throwable) {
                logFailed(record, properties.audit)
                throw error
            }
        }

    fun processCauldronTelemetry(record: ConsumerRecord<String, CauldronTelemetryEvent>): Mono<Boolean> =
        mono(context = workerDispatcher) {
            try {
                if (shouldDiscard(properties.consumers.telemetry, record)) return@mono processingCompleted()
                if (properties.consumers.processingEnabled) {
                    cauldronTelemetryService.recalculate(record.value())
                } else {
                    latencyOnlyDelay()
                }
                logProcessed(record, properties.audit)
                logger.debug(
                    "Confluent Parallel Reactor cauldron event received for key={}, cauldron={}",
                    record.key(),
                    record.value().cauldronId
                )
                processingCompleted()
            } catch (error: Throwable) {
                logFailed(record, properties.audit)
                throw error
            }
        }

    private suspend fun latencyOnlyDelay() {
        delay((5L..8L).random())
    }

    // ReactorProcessor 0.5.3.3 acknowledges successful work on onNext, so the publisher must emit one value.
    private fun processingCompleted(): Boolean = true

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
}
