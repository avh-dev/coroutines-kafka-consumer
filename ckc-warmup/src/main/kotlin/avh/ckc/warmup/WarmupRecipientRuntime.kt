package avh.ckc.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.extension.ConsumerRuntimeFactory
import avh.ckc.core.isFatalJvmError
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.warmup.protocol.WarmupSessionId
import java.util.UUID
import kotlin.time.Duration
import kotlinx.coroutines.CancellationException
import org.apache.kafka.common.TopicPartition

internal fun warmupRecipientClientId(sessionId: WarmupSessionId, start: WarmupDestinationStart): String {
    val assignment = start.assignment
    val destination = assignment.destination
    val connectionIdentity = listOf(
        assignment.mapping.participantId.value,
        sessionId.value,
        destination.topic(),
        destination.partition().toString()
    ).joinToString("\u0000")
    return "ckc.traffic-warmup.v6.recipient." +
        UUID.nameUUIDFromBytes(connectionIdentity.toByteArray(Charsets.UTF_8))
}

internal class CoroutinesPreparedWarmupRecipientConsumer(
    start: WarmupDestinationStart,
    private val runtime: CoroutinesKafkaConsumer<*, *>
) : PreparedWarmupRecipientConsumer {
    override val assignment = start.assignment
    override val startOffset = start.offset

    override fun start() = runtime.start()
    override suspend fun awaitReady() = runtime.awaitReady()
    override suspend fun awaitTermination() = runtime.awaitTermination()
    override suspend fun abort(): Boolean = abortWarmupRecipientRuntime(runtime)
    override suspend fun stop() {
        try {
            runtime.stop()
        } catch (error: CancellationException) {
            if (!runtime.runtimeControl.isShutdownComplete()) throw error
            throw WarmupRecipientConsumerStoppedWithFailureException(error)
        } catch (error: Throwable) {
            if (error.isFatalJvmError()) throw error
            if (!runtime.runtimeControl.isShutdownComplete()) throw error
            throw WarmupRecipientConsumerStoppedWithFailureException(error)
        }
    }
}

internal suspend fun abortWarmupRecipientRuntime(runtime: CoroutinesKafkaConsumer<*, *>): Boolean {
    if (runtime.runtimeControl.isShutdownComplete()) return true
    try {
        runtime.runtimeControl.abort(WarmupRecipientCleanupTimeoutException())
    } catch (error: CancellationException) {
        if (!runtime.runtimeControl.isShutdownComplete()) throw error
        throw WarmupRecipientConsumerStoppedWithFailureException(error)
    } catch (error: Throwable) {
        if (error.isFatalJvmError() || !runtime.runtimeControl.isShutdownComplete()) throw error
        throw WarmupRecipientConsumerStoppedWithFailureException(error)
    }
    return runtime.runtimeControl.isShutdownComplete()
}

internal class WarmupRecipientCleanupTimeoutException :
    IllegalStateException("Warm-up recipient consumer cleanup deadline expired")

internal class WarmupRecipientConsumerStoppedWithFailureException(cause: Throwable) :
    IllegalStateException("Warm-up recipient consumer stopped with a terminal failure", cause)

internal fun <K, V> ConsumerRuntimeFactory<K, V>.buildWarmupRecipient(
    consumerProperties: Map<String, Any?>,
    sessionId: WarmupSessionId,
    topicPartition: TopicPartition,
    startOffset: Long,
    completionPublisher: WarmupCompletionPublisher? = null,
    resultAdmission: WarmupResultAdmission? = null,
    runtimeMetrics: ConsumerMetrics<K, V> = metrics,
    runtimeShutdownTimeout: Duration = Duration.INFINITE
): CoroutinesKafkaConsumer<K, V> = assigned(
    consumerProperties = consumerProperties,
    startOffsets = mapOf(topicPartition to startOffset),
    admission = WarmupRecipientAdmission(sessionId, resultAdmission),
    metrics = completionPublisher?.let { WarmupRecipientOutcomeMetrics(sessionId, it, runtimeMetrics) } ?: runtimeMetrics,
    shutdownTimeout = runtimeShutdownTimeout
)
