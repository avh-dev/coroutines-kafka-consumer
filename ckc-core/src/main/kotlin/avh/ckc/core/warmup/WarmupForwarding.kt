package avh.ckc.core.warmup

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeaders
import java.time.Duration as JavaDuration
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class WarmupForwardingConfiguration(
    val sessionId: WarmupSessionId,
    val consumerId: WarmupConsumerId,
    assignments: Collection<WarmupPartitionAssignment>,
    val profile: WarmupForwardingProfile,
    producerProperties: Map<String, Any?>,
    val maxInFlightSends: Int = 32,
    val producerCloseTimeout: Duration = 5.seconds
) {
    val assignments: List<WarmupPartitionAssignment> = immutableListSnapshot(assignments)
    val producerProperties: Map<String, Any?> = immutableMapSnapshot(producerProperties)

    init {
        require(this.assignments.isNotEmpty()) { "assignments must not be empty" }
        require(this.assignments.all { it.consumerId == consumerId }) {
            "All forwarding assignments must belong to consumerId $consumerId"
        }
        val duplicateSources = this.assignments.groupingBy(WarmupPartitionAssignment::sourceTopic)
            .eachCount()
            .filterValues { it > 1 }
            .keys
        require(duplicateSources.isEmpty()) {
            "Each source topic must have one forwarding assignment; duplicates: ${duplicateSources.sorted()}"
        }
        require(this.producerProperties.containsKey(KEY_SERIALIZER_CLASS_CONFIG)) {
            "Kafka property '$KEY_SERIALIZER_CLASS_CONFIG' must be specified for warm-up forwarding"
        }
        require(this.producerProperties.containsKey(VALUE_SERIALIZER_CLASS_CONFIG)) {
            "Kafka property '$VALUE_SERIALIZER_CLASS_CONFIG' must be specified for warm-up forwarding"
        }
        require(maxInFlightSends > 0) { "maxInFlightSends must be > 0" }
        require(producerCloseTimeout.isPositive()) { "producerCloseTimeout must be > 0" }
    }
}

data class WarmupForwardingAcknowledgement(
    val sessionId: WarmupSessionId,
    val consumerId: WarmupConsumerId,
    val sourceTopicPartition: TopicPartition,
    val sourceOffset: Long,
    val warmupTopicPartition: TopicPartition,
    val warmupOffset: Long
) {
    init {
        require(sourceOffset >= 0) { "sourceOffset must not be negative" }
        require(warmupOffset >= 0) { "warmupOffset must not be negative" }
    }
}

fun interface WarmupForwardingObserver {
    suspend fun onForwarded(acknowledgement: WarmupForwardingAcknowledgement)

    companion object {
        val NOOP: WarmupForwardingObserver = WarmupForwardingObserver { }
    }
}

interface WarmupRecordForwarding<K, V> {
    /** Initializes forwarding resources before consumer workers start. */
    fun start() = Unit

    /** Returns true only after the selected record and its forwarding audit event are acknowledged. */
    suspend fun forwardIfSelected(record: ConsumerRecord<K, V>): Boolean

    suspend fun close()
}

fun <K, V> kafkaWarmupRecordForwarding(
    configuration: WarmupForwardingConfiguration,
    observer: WarmupForwardingObserver = WarmupForwardingObserver.NOOP
): WarmupRecordForwarding<K, V> = KafkaWarmupRecordForwarding(
    configuration = configuration,
    observer = observer,
    producerFactory = { KafkaProducer(configuration.producerProperties) }
)

internal class KafkaWarmupRecordForwarding<K, V>(
    private val configuration: WarmupForwardingConfiguration,
    private val observer: WarmupForwardingObserver,
    private val producerFactory: () -> Producer<K, V>,
    private val selector: WarmupForwardingSelector = WarmupForwardingSelector(configuration.profile)
) : WarmupRecordForwarding<K, V> {
    private val assignmentsBySource = configuration.assignments.associateBy { it.sourceTopic }
    private val sendPermits = Semaphore(configuration.maxInFlightSends)
    private val closed = AtomicBoolean(false)
    @Volatile
    private var producer: Producer<K, V>? = null

    override fun start() {
        producer()
    }

    override suspend fun forwardIfSelected(record: ConsumerRecord<K, V>): Boolean {
        val assignment = assignmentsBySource[record.topic()] ?: return false
        if (!selector.shouldForward()) return false
        check(!closed.get()) { "Warm-up forwarding is closed" }

        return sendPermits.withPermit {
            val metadata = send(record.toWarmupProducerRecord(assignment))
            observer.onForwarded(record.acknowledgement(metadata))
            true
        }
    }

    override suspend fun close() {
        if (closed.compareAndSet(false, true)) {
            withContext(Dispatchers.IO) {
                producer?.close(
                    JavaDuration.ofMillis(configuration.producerCloseTimeout.inWholeMilliseconds.coerceAtLeast(1L))
                )
            }
        }
    }

    private suspend fun send(record: ProducerRecord<K, V>): RecordMetadata =
        suspendCancellableCoroutine { continuation ->
            val completionClaimed = AtomicBoolean(false)
            fun complete(result: Result<RecordMetadata>) {
                if (completionClaimed.compareAndSet(false, true)) continuation.resumeWith(result)
            }
            try {
                producer().send(record) { metadata, error ->
                    when {
                        error != null -> complete(Result.failure(error))
                        metadata == null -> complete(Result.failure(
                            IllegalStateException("Warm-up producer acknowledgement did not contain record metadata")
                        ))
                        else -> complete(Result.success(metadata))
                    }
                }
            } catch (error: Throwable) {
                complete(Result.failure(error))
            }
        }

    private fun producer(): Producer<K, V> = producer ?: synchronized(this) {
        producer ?: run {
            check(!closed.get()) { "Warm-up forwarding is closed" }
            producerFactory().also { producer = it }
        }
    }

    private fun ConsumerRecord<K, V>.toWarmupProducerRecord(
        assignment: WarmupPartitionAssignment
    ): ProducerRecord<K, V> = ProducerRecord(
        assignment.warmupTopic,
        assignment.warmupPartition,
        timestamp().takeIf { it >= 0L },
        key(),
        value(),
        RecordHeaders(headers().toArray())
    )

    private fun ConsumerRecord<K, V>.acknowledgement(metadata: RecordMetadata) =
        WarmupForwardingAcknowledgement(
            sessionId = configuration.sessionId,
            consumerId = configuration.consumerId,
            sourceTopicPartition = TopicPartition(topic(), partition()),
            sourceOffset = offset(),
            warmupTopicPartition = TopicPartition(metadata.topic(), metadata.partition()),
            warmupOffset = metadata.offset()
        )
}

internal class WarmupForwardingSelector(
    private val profile: WarmupForwardingProfile,
    private val nanoTime: () -> Long = System::nanoTime,
    private val randomDouble: () -> Double = { ThreadLocalRandom.current().nextDouble() }
) {
    private val startedAtNanos = AtomicLong(UNSTARTED)

    fun shouldForward(): Boolean {
        val now = nanoTime()
        val observedStart = startedAtNanos.updateAndGet { current ->
            if (current == UNSTARTED) now else current
        }
        val fraction = if (profile.rampUpDuration == Duration.ZERO) {
            profile.targetFraction
        } else {
            val progress = ((now - observedStart).coerceAtLeast(0L).toDouble() /
                profile.rampUpDuration.inWholeNanoseconds.toDouble()).coerceIn(0.0, 1.0)
            profile.initialFraction + (profile.targetFraction - profile.initialFraction) * progress
        }
        return randomDouble() < fraction
    }

    private companion object {
        const val UNSTARTED = Long.MIN_VALUE
    }
}
