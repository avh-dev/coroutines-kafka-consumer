package avh.ckc.core

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.errors.RetriableException
import java.io.IOException
import kotlinx.coroutines.CoroutineDispatcher

/**
 * Record processing pipeline executed by worker coroutines.
 *
 * Responsibilities:
 * - deserialize raw Kafka records on a dedicated dispatcher;
 * - retry transient deserialization failures with an internal policy;
 * - apply user-configured retry logic to the business handler;
 * - delegate exhausted processing failures to [processingFailureHandler];
 * - mark offsets as processed for backpressure-based commit tracking.
 */
internal class RecordProcessor<K, V>(
    private val deliveryStrategy: DeliveryStrategy,
    private val deserializationDispatcher: CoroutineDispatcher,
    private val handler: KafkaRecordHandler<K, V>,
    private val retryPolicy: RetryPolicy,
    private val telemetry: ConsumerTelemetry<K, V>,
    private val processingFailureHandler: ProcessingFailureHandler<K, V>,
    private val partitionRegistry: PartitionRegistry
) {
    /**
     * Processes a single raw Kafka record end-to-end.
     */
    suspend fun process(
        record: ConsumerRecord<ByteArray, ByteArray>,
        deserializers: WorkerDeserializers<K, V>
    ) {
        val startedAt = System.nanoTime()
        val recordAgeMillis = (System.currentTimeMillis() - record.timestamp()).coerceAtLeast(0L)
        try {
            val (key, value) = deserializeWithRetry(record, deserializers)

            try {
                executeWithRetry(record, key, value) {
                    handler.process(key, value, record)
                }
            } catch (error: Throwable) {
                if (error.isCancellation()) {
                    throw error
                }

                processingFailureHandler.handle(key, value, record, error)
                telemetry.onRecordFailed(
                    key = key,
                    value = value,
                    record = record,
                    recordAgeMillis = recordAgeMillis,
                    error = error,
                    durationNanos = System.nanoTime() - startedAt
                )
                return
            }

            if (deliveryStrategy == DeliveryStrategy.BACKPRESSURE) {
                partitionRegistry.partitionStateFor(record)?.markProcessed(record.offset())
            }
            telemetry.onRecordProcessed(
                key = key,
                value = value,
                record = record,
                recordAgeMillis = recordAgeMillis,
                durationNanos = System.nanoTime() - startedAt
            )
        } catch (error: Throwable) {
            if (error.isCancellation()) {
                throw error
            }
            telemetry.onRecordFailed(
                key = null,
                value = null,
                record = record,
                recordAgeMillis = recordAgeMillis,
                error = error,
                durationNanos = System.nanoTime() - startedAt
            )
            throw error
        }
    }

    /**
     * Deserializes key and value with a small internal retry loop for transient failures.
     */
    private suspend fun deserializeWithRetry(
        record: ConsumerRecord<ByteArray, ByteArray>,
        deserializers: WorkerDeserializers<K, V>
    ): Pair<K?, V?> {
        var retries = 0

        while (true) {
            try {
                return withContext(deserializationDispatcher) {
                    deserializers.keyDeserializer.deserialize(record.topic(), record.headers(), record.key()) to
                            deserializers.valueDeserializer.deserialize(record.topic(), record.headers(), record.value())
                }
            } catch (error: Throwable) {
                if (error.isCancellation()) {
                    throw error
                }

                if (!error.isTransientDeserializationFailure() || retries >= DESERIALIZATION_MAX_RETRIES) {
                    throw error
                }

                retries++
                delay(DESERIALIZATION_RETRY_DELAY_MS)
            }
        }
    }

    /**
     * Executes the user handler with the externally configured retry policy.
     */
    private suspend fun executeWithRetry(
        record: ConsumerRecord<ByteArray, ByteArray>,
        key: K?,
        value: V?,
        block: suspend () -> Unit
    ) {
        var retries = 0

        while (true) {
            try {
                block()
                return
            } catch (error: Throwable) {
                if (error.isCancellation()) {
                    throw error
                }

                val rule = retryPolicy.ruleFor(error) ?: throw error
                if (retries >= rule.maxRetries) {
                    throw error
                }

                retries++
                telemetry.onRetry(key = key, value = value, record = record, attempt = retries, error = error)
                if (rule.delay.isPositive()) {
                    delay(rule.delay)
                }
            }
        }
    }

    /**
     * Walks the exception cause chain and classifies schema-registry/network style failures
     * as transient so they can be retried inside the deserialization phase.
     */
    private fun Throwable.isTransientDeserializationFailure(): Boolean {
        var current: Throwable? = this
        while (current != null) {
            if (current is RetriableException || current is IOException) {
                return true
            }
            current = current.cause
        }
        return false
    }

    private fun Throwable.isCancellation(): Boolean = this is CancellationException

    private companion object {
        const val DESERIALIZATION_MAX_RETRIES = 3
        const val DESERIALIZATION_RETRY_DELAY_MS = 250L
    }
}
