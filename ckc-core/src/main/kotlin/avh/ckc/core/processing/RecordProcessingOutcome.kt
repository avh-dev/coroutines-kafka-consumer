package avh.ckc.core.processing

import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.metrics.RecordHandoffReason
import org.apache.kafka.clients.consumer.ConsumerRecord

/** Semantic result of one record after runtime admission. */
internal sealed interface RecordProcessingOutcome {
    data object HandlerSucceeded : RecordProcessingOutcome

    /** The handler exhausted retries and the configured failure handler completed. */
    data class HandlerFailed(val error: Throwable) : RecordProcessingOutcome

    data class PolicyDiscarded(val reason: RecordDropReason) : RecordProcessingOutcome

    /** The record remains non-terminal and may be replayed by another owner. */
    data class HandedOff(val reason: RecordHandoffReason) : RecordProcessingOutcome
}

/**
 * One mode-independent seam around the complete local handler/retry/failure pipeline.
 * Implementations may choose another execution path or invoke [processLocally] as fallback.
 */
internal fun interface AdmittedRecordInterceptor<K, V> {
    suspend fun process(
        record: ConsumerRecord<K, V>,
        processLocally: suspend () -> RecordProcessingOutcome
    ): RecordProcessingOutcome
}

internal fun <K, V> localRecordInterceptor(): AdmittedRecordInterceptor<K, V> =
    AdmittedRecordInterceptor { _, processLocally -> processLocally() }

/** Receives every semantic result after its ordinary outcome metrics are applied. */
internal fun interface RecordProcessingOutcomeObserver<K, V> {
    fun onOutcome(record: ConsumerRecord<K, V>, outcome: RecordProcessingOutcome)
}

internal fun <K, V> noOpRecordProcessingOutcomeObserver(): RecordProcessingOutcomeObserver<K, V> =
    RecordProcessingOutcomeObserver { _, _ -> }
