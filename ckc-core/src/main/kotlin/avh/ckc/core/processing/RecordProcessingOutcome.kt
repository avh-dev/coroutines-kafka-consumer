package avh.ckc.core.processing

import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.metrics.RecordHandoffReason
import org.apache.kafka.clients.consumer.ConsumerRecord

/** Semantic result of one record after admission to a processing runtime. */
internal sealed interface RecordProcessingOutcome {
    data object HandlerSucceeded : RecordProcessingOutcome

    /** The recipient ran the handler and reported success to the donor. */
    data object HandlerSucceededRemotely : RecordProcessingOutcome

    /** The handler exhausted retries and the configured failure handler completed. */
    data class HandlerFailed(val error: Throwable) : RecordProcessingOutcome

    data class PolicyDiscarded(val reason: RecordDropReason) : RecordProcessingOutcome

    /** The record remains non-terminal and may be replayed by another owner. */
    data class HandedOff(val reason: RecordHandoffReason) : RecordProcessingOutcome
}

/**
 * Mode-independent seam around the complete local handler, retry, and failure pipeline.
 * An interceptor may delegate the record or invoke [processLocally] as fallback.
 */
internal fun interface AdmittedRecordInterceptor<K, V> {
    suspend fun process(
        record: ConsumerRecord<K, V>,
        processLocally: suspend () -> RecordProcessingOutcome
    ): RecordProcessingOutcome
}

internal fun <K, V> localRecordInterceptor(): AdmittedRecordInterceptor<K, V> =
    AdmittedRecordInterceptor { _, processLocally -> processLocally() }
