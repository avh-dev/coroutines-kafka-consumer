package avh.ckc.core.processing.runtime

import avh.ckc.core.KafkaRecordHandler
import avh.ckc.core.ProcessingFailureHandler
import avh.ckc.core.RetryPolicy
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.ConsumerRuntimeStatsTracker
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.processing.ProcessedRecordTracker
import avh.ckc.core.processing.RecordProcessingRuntime
import avh.ckc.core.processing.RecordProcessor
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration

/**
 * Freshness-first runtime that coalesces queued records by deserialized Kafka key.
 *
 * The channel contains envelopes, not raw records. A key may have at most one queued envelope in [states]:
 * newer records for the same queued key replace the envelope payload, while records for new keys are admitted
 * only while [admissionBudget] has room. In-flight records are never replaced under a worker; if a newer same-key
 * record arrives while the previous one is being processed, it becomes the single queued successor.
 *
 * Offset tracking is intentionally disabled by the consumer wiring for this runtime. Drops rely on Kafka
 * auto-commit semantics, matching the broader freshness-first contract.
 */
internal class FreshnessFirstReplacePendingByKeyRecordProcessingRuntime<K, V>(
    private val workerConcurrency: Int,
    workChannelCapacity: Int,
    freshnessMaxRecordAge: Duration?,
    private val processingDispatcher: CoroutineDispatcher,
    private val scope: CoroutineScope,
    private val metrics: ConsumerMetrics<K, V>,
    handler: KafkaRecordHandler<K, V>,
    retryPolicy: RetryPolicy,
    processingFailureHandler: ProcessingFailureHandler<K, V>,
    private val processedRecordTracker: ProcessedRecordTracker
) : RecordProcessingRuntime<K, V> {
    private val recordProcessor = RecordProcessor(
        handler = handler,
        retryPolicy = retryPolicy,
        metrics = metrics,
        processingFailureHandler = processingFailureHandler,
        onRecordProcessed = processedRecordTracker::markProcessed
    )
    private val recordDropPolicy = FreshnessRecordAgeDropPolicy(freshnessMaxRecordAge, metrics)
    private val runtimeStats = ConsumerRuntimeStatsTracker(
        workerCount = workerConcurrency,
        workQueueCapacity = workChannelCapacity
    )
    private val admissionBudget = AdmissionBudget(workChannelCapacity, runtimeStats)
    private val states = ConcurrentHashMap<FreshnessKey, KeyState<K, V>>()
    private val acceptingRecords = AtomicBoolean(true)
    private val workChannel by lazy {
        Channel<Envelope<K, V>>(
            capacity = workChannelCapacity,
            onBufferOverflow = BufferOverflow.SUSPEND,
            onUndeliveredElement = ::releaseQueuedEnvelope
        )
    }

    private var workerJobs: List<Job> = emptyList()

    override fun start(onFailure: (Throwable) -> Unit) {
        workerJobs = List(workerConcurrency) { workerIndex ->
            scope.launch(processingDispatcher + CoroutineName("KafkaFreshnessByKeyWorker-$workerIndex")) {
                runWorker()
            }.also { job ->
                job.invokeOnCompletion { cause ->
                    if (cause != null && cause !is CancellationException) {
                        close(cause)
                        onFailure(cause)
                    }
                }
            }
        }

        metrics.bindRuntimeMetrics(runtimeStats)
    }

    override fun tryEmit(record: ConsumerRecord<K, V>): Boolean {
        if (processedRecordTracker.isProcessed(record)) {
            metrics.onRecordDropped(record, RecordDropReason.ALREADY_PROCESSED)
            return true
        }
        if (!acceptingRecords.get()) {
            return false
        }

        val key = keyFor(record)
        var envelopeToSend: Envelope<K, V>? = null
        var replacedRecord: ConsumerRecord<K, V>? = null
        var dropIncoming = false

        // All per-key transitions happen under ConcurrentHashMap.compute() so admission, replacement,
        // and worker claim/finish cannot remove or overwrite each other's state out of order.
        states.compute(key) { _, current ->
            val state = current ?: KeyState()
            val queued = state.queued
            if (queued != null) {
                replacedRecord = queued.record
                queued.record = record
                state
            } else if (admissionBudget.tryAcquire()) {
                val envelope = Envelope(key, record)
                state.queued = envelope
                envelopeToSend = envelope
                state
            } else {
                dropIncoming = true
                current
            }
        }

        replacedRecord?.let {
            metrics.onRecordDropped(it, RecordDropReason.REPLACED_BY_NEWER_KEY_RECORD)
        }
        if (dropIncoming) {
            metrics.onRecordDropped(record, RecordDropReason.NEW_KEY_QUEUE_FULL)
            return true
        }

        val envelope = envelopeToSend ?: return true
        val result = workChannel.trySend(envelope)
        if (!result.isSuccess) {
            releaseQueuedEnvelope(envelope)
        }
        return result.isSuccess
    }

    override fun close(cause: Throwable?) {
        acceptingRecords.set(false)
        workChannel.close(cause)
        releaseAllQueuedEnvelopes()
    }

    override fun stateSnapshot() = runtimeStats.snapshot()

    override suspend fun stop() {
        acceptingRecords.set(false)
        try {
            workChannel.close()
            workerJobs.joinAll()
        } finally {
            releaseAllQueuedEnvelopes()
            metrics.unbindRuntimeMetrics()
        }
    }

    private suspend fun runWorker() {
        while (currentCoroutineContext().isActive) {
            val envelope = workChannel.receiveCatching().getOrNull() ?: break
            val record = claim(envelope) ?: continue
            runtimeStats.onWorkerStarted()
            try {
                if (!recordDropPolicy.shouldDrop(record)) {
                    recordProcessor.process(record)
                }
            } finally {
                runtimeStats.onWorkerFinished()
                finish(envelope.key)
            }
        }
    }

    private fun claim(envelope: Envelope<K, V>): ConsumerRecord<K, V>? {
        var record: ConsumerRecord<K, V>? = null
        states.compute(envelope.key) { _, state ->
            if (state == null || state.queued !== envelope) {
                state
            } else {
                // Capture the record snapshot before processing so later arrivals for the same key create
                // or replace a queued successor instead of mutating the record being handled.
                state.queued = null
                state.inFlight = true
                record = envelope.record
                admissionBudget.release()
                state
            }
        }
        return record
    }

    private fun finish(key: FreshnessKey) {
        states.compute(key) { _, state ->
            if (state == null) {
                null
            } else {
                // Remove idle keys from the map. If a successor arrived while this record was in-flight,
                // keep the state so the already-queued envelope can be claimed by a worker.
                state.inFlight = false
                if (state.queued == null) null else state
            }
        }
    }

    private fun releaseQueuedEnvelope(envelope: Envelope<K, V>) {
        var released = false
        states.compute(envelope.key) { _, state ->
            if (state == null || state.queued !== envelope) {
                state
            } else {
                // Channel cancellation/close may report an undelivered envelope. Release capacity only if this
                // exact envelope is still the current queued representative for its key.
                state.queued = null
                released = true
                if (state.inFlight) state else null
            }
        }
        if (released) {
            admissionBudget.release()
        }
    }

    private fun releaseAllQueuedEnvelopes() {
        states.forEach { (key, _) ->
            states.compute(key) { _, state ->
                if (state != null && state.queued != null) {
                    state.queued = null
                    admissionBudget.release()
                }
                null
            }
        }
    }

    private fun keyFor(record: ConsumerRecord<K, V>): FreshnessKey {
        val key = record.key()
        return if (key == null) {
            FreshnessKey.NullKey
        } else {
            FreshnessKey.DeserializedKey(key)
        }
    }

    private class KeyState<K, V>(
        var queued: Envelope<K, V>? = null,
        var inFlight: Boolean = false
    )

    private class Envelope<K, V>(
        val key: FreshnessKey,
        var record: ConsumerRecord<K, V>
    )

    private sealed interface FreshnessKey {
        data object NullKey : FreshnessKey

        data class DeserializedKey(val key: Any) : FreshnessKey
    }

    private class AdmissionBudget(
        private val capacity: Int,
        private val runtimeStats: ConsumerRuntimeStatsTracker
    ) {
        private val used = AtomicInteger(0)

        fun tryAcquire(): Boolean {
            while (true) {
                val current = used.get()
                if (current >= capacity) {
                    return false
                }
                if (used.compareAndSet(current, current + 1)) {
                    runtimeStats.onWorkEnqueued()
                    return true
                }
            }
        }

        fun release() {
            val released = used.getAndUpdate { current ->
                if (current > 0) current - 1 else 0
            }
            if (released > 0) {
                runtimeStats.onWorkDequeued()
            }
        }
    }
}
