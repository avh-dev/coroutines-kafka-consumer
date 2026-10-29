package avh.ckc.core.processing

import avh.ckc.core.KafkaRecordHandler
import avh.ckc.core.ProcessingFailureHandler
import avh.ckc.core.RecordProcessingContext
import avh.ckc.core.RetryPolicy
import avh.ckc.core.metrics.ConsumerMetrics
import kotlinx.coroutines.CancellationException
import org.apache.kafka.clients.consumer.ConsumerRecord
import kotlin.time.Duration

/**
 * Record processing pipeline executed by worker coroutines.
 *
 * Responsibilities:
 * - apply user-configured retry logic to the business handler;
 * - delegate exhausted processing failures to [processingFailureHandler];
 * - notify the owner after successful processing.
 */
internal class RecordProcessor<K, V>(
    private val handler: KafkaRecordHandler<K, V>,
    private val retryPolicy: RetryPolicy,
    private val metrics: ConsumerMetrics<K, V>,
    private val processingFailureHandler: ProcessingFailureHandler<K, V>,
    private val onRecordProcessed: (ConsumerRecord<K, V>) -> Unit,
    private val awaitRetryPermission: suspend (ConsumerRecord<K, V>, Duration) -> Boolean = { _, delay ->
        if (delay.isPositive()) kotlinx.coroutines.delay(delay)
        true
    },
    private val recordProcessingContext: RecordProcessingContext<K, V>? = null,
    private val currentTimeMillis: () -> Long = System::currentTimeMillis
) {
    /**
     * Processes a single Kafka record end-to-end.
     */
    suspend fun process(record: ConsumerRecord<K, V>): RecordProcessingOutcome {
        if (recordProcessingContext == null) return processDirect(record)

        var outcome: RecordProcessingOutcome? = null
        recordProcessingContext.withRecordContext(record) {
            outcome = processDirect(record)
        }
        return checkNotNull(outcome) { "Record processing context did not execute its block" }
    }

    private suspend fun processDirect(record: ConsumerRecord<K, V>): RecordProcessingOutcome {
        val startedAt = System.nanoTime()
        val key = record.key()
        val value = record.value()
        try {
            try {
                val completed = executeWithRetry(record, key, value) {
                    handler.process(record)
                }
                if (!completed) return RecordProcessingOutcome.HANDED_OFF
            } catch (error: Throwable) {
                if (error.isCancellation()) {
                    throw error
                }

                processingFailureHandler.handle(record, error)
                onRecordProcessed(record)
                metrics.onRecordFailed(
                    key = key,
                    value = value,
                    record = record,
                    error = error,
                    durationNanos = System.nanoTime() - startedAt
                )
                return RecordProcessingOutcome.COMPLETED
            }

            onRecordProcessed(record)
            metrics.onRecordProcessed(
                key = key,
                value = value,
                record = record,
                endToEndLatencyMillis = (currentTimeMillis() - record.timestamp()).coerceAtLeast(0L),
                durationNanos = System.nanoTime() - startedAt
            )
            return RecordProcessingOutcome.COMPLETED
        } catch (error: Throwable) {
            if (error.isCancellation()) {
                throw error
            }
            metrics.onRecordFailed(
                key = null,
                value = null,
                record = record,
                error = error,
                durationNanos = System.nanoTime() - startedAt
            )
            throw error
        }
    }

    /**
     * Executes the user handler with the externally configured retry policy.
     */
    private suspend fun executeWithRetry(
        record: ConsumerRecord<K, V>,
        key: K?,
        value: V?,
        block: suspend () -> Unit
    ): Boolean {
        var retries = 0

        while (true) {
            try {
                block()
                return true
            } catch (error: Throwable) {
                if (error.isCancellation()) {
                    throw error
                }

                val rule = retryPolicy.ruleFor(error) ?: throw error
                if (retries >= rule.maxRetries) {
                    throw error
                }

                // Revoke closes retry admission without treating the record as processed or failed.
                // The next owner will replay it from Kafka's committed offset.
                if (!awaitRetryPermission(record, rule.delay)) return false
                retries++
                metrics.onRetry(key = key, value = value, record = record, attempt = retries, error = error)
            }
        }
    }

    private fun Throwable.isCancellation(): Boolean = this is CancellationException
}

internal enum class RecordProcessingOutcome {
    COMPLETED,
    HANDED_OFF
}
