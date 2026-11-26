package avh.ckc.core.processing

import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.metrics.RecordHandoffReason
import org.apache.kafka.clients.consumer.ConsumerRecord

/** Semantic result of one record after admission to a processing runtime. */
sealed interface RecordProcessingOutcome {
    /** The local handler completed successfully. */
    data object HandlerSucceeded : RecordProcessingOutcome

    /** An external processor ran the handler and reported success. */
    data object HandlerSucceededRemotely : RecordProcessingOutcome

    /** The handler exhausted retries and the configured failure handler completed. */
    data class HandlerFailed(val error: Throwable) : RecordProcessingOutcome

    /** The configured processing policy completed the record by discarding it. */
    data class PolicyDiscarded(val reason: RecordDropReason) : RecordProcessingOutcome

    /** The record remains non-terminal and may be replayed by another owner. */
    data class HandedOff(val reason: RecordHandoffReason) : RecordProcessingOutcome
}

/**
 * Mode-independent seam around the complete local handler, retry, and failure pipeline.
 * An interceptor can delegate the record or invoke the supplied local pipeline as fallback.
 * The local pipeline checks ownership again before processing.
 */
fun interface AdmittedRecordInterceptor<K, V> {
    /**
     * Runs in the record's processing coroutine and occupies its ordering slot until the returned
     * outcome is known. The local callback is valid only during this invocation. Call it at most
     * once and return its outcome if used. Propagate cancellation so ownership handoff can proceed.
     */
    suspend fun process(
        record: ConsumerRecord<K, V>,
        processLocally: suspend () -> RecordProcessingOutcome
    ): RecordProcessingOutcome
}

internal fun <K, V> localRecordInterceptor(): AdmittedRecordInterceptor<K, V> =
    AdmittedRecordInterceptor { _, processLocally -> processLocally() }
