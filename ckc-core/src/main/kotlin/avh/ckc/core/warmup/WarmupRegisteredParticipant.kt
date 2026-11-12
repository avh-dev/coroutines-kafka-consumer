package avh.ckc.core.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.processing.RecordProcessingOutcomeObserver
import kotlinx.coroutines.CoroutineScope
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration

internal interface PreparedWarmupDonor {
    val sessionId: WarmupSessionId
    val assignments: List<WarmupPartitionAssignment>
    fun activate()
    fun accept(completed: WarmupRecordCompleted): WarmupResultTracker.Completion
    suspend fun stopAndAwaitSends(timeout: Duration): List<WarmupFinalOffset>
    fun finish()
}

internal interface PreparedWarmupRecipient {
    val assignments: List<WarmupPartitionAssignment>
    suspend fun start()
    suspend fun drain(finalOffsets: Collection<WarmupFinalOffset>)
    suspend fun startMain()
    suspend fun stop()
}

internal interface RegisteredWarmupParticipant {
    val consumerId: WarmupConsumerId
    val mappings: List<WarmupTopicMapping>
    fun donorMappings(requested: Collection<WarmupTopicMapping>): List<WarmupDonorMapping>
    fun prepareDonor(
        sessionId: WarmupSessionId,
        assignments: List<WarmupPartitionAssignment>
    ): PreparedWarmupDonor
    fun prepareRecipient(
        sessionId: WarmupSessionId,
        assignments: List<WarmupPartitionAssignment>,
        donorByDestination: Map<TopicPartition, WarmupInstanceId>,
        sendCompletion: suspend (WarmupInstanceId, WarmupRecordCompleted) -> Unit
    ): PreparedWarmupRecipient
    suspend fun startMain()
    suspend fun stop()
    suspend fun awaitTermination()
}

internal class KafkaWarmupParticipant<K, V>(
    override val consumerId: WarmupConsumerId,
    override val mappings: List<WarmupTopicMapping>,
    private val main: CoroutinesKafkaConsumer<K, V>,
    private val switchableDataPlane: SwitchableWarmupDataPlane<K, V>,
    private val createWarmup: (
        Set<TopicPartition>,
        RecordProcessingOutcomeObserver<K, V>
    ) -> CoroutinesKafkaConsumer<K, V>,
    private val producerProperties: Map<String, Any?>,
    private val metrics: ConsumerMetrics<K, V>,
    private val forwardingObserver: WarmupForwardingObserver,
    private val scope: CoroutineScope,
    private val quota: Int,
    private val initialQuota: Int,
    private val quotaInterval: Duration,
    private val quotaRampDuration: Duration,
    private val delegationTimeout: Duration,
    private val cleanupTimeout: Duration,
    private val completionBufferCapacity: Int
) : RegisteredWarmupParticipant {
    private val mainStarted = AtomicBoolean(false)
    private val mappingBySource = mappings.associateBy(WarmupTopicMapping::sourceTopic)

    override fun donorMappings(requested: Collection<WarmupTopicMapping>): List<WarmupDonorMapping> {
        if (!mainStarted.get()) return emptyList()
        val assignedByTopic = main.stateSnapshot().pollLoops
            .flatMap { it.assignedPartitions }
            .groupBy({ it.topic }, { it.partition })
        return requested.mapNotNull { candidate ->
            val local = mappingBySource[candidate.sourceTopic]
            val partitions = assignedByTopic[candidate.sourceTopic].orEmpty().toSet()
            if (local == candidate && partitions.isNotEmpty()) {
                WarmupDonorMapping(candidate, partitions)
            } else {
                null
            }
        }
    }

    override fun prepareDonor(
        sessionId: WarmupSessionId,
        assignments: List<WarmupPartitionAssignment>
    ): PreparedWarmupDonor {
        require(assignments.isNotEmpty() && assignments.all { it.mapping.consumerId == consumerId }) {
            "Invalid donor assignments for $consumerId"
        }
        val destinations = assignments.associate { assignment ->
            require(mappingBySource[assignment.mapping.sourceTopic] == assignment.mapping) {
                "Donor mapping does not match local consumer $consumerId"
            }
            assignment.mapping.sourceTopic to WarmupDestination(
                assignment.mapping.warmupTopic,
                assignment.warmupPartition
            )
        }
        val plane = FixedLinkWarmupDataPlane(
            sessionId = sessionId,
            quota = quota,
            initialQuota = initialQuota,
            quotaInterval = quotaInterval,
            quotaRampDuration = quotaRampDuration,
            delegationTimeout = delegationTimeout,
            destinationResolver = WarmupDestinationResolver { record -> destinations[record.topic()] },
            sender = KafkaWarmupRecordSender(producerProperties),
            forwardingObserver = forwardingObserver,
            metrics = metrics,
            supportsRemoteDiscard = ::supportsRemoteDiscard
        )
        return object : PreparedWarmupDonor {
            override val sessionId = sessionId
            override val assignments = assignments.toList()

            override fun activate() = switchableDataPlane.activate(plane)

            override fun accept(completed: WarmupRecordCompleted) = switchableDataPlane.accept(completed)

            override suspend fun stopAndAwaitSends(timeout: Duration): List<WarmupFinalOffset> {
                val acknowledged = plane.stopDelegatingAndAwaitSends(timeout)
                    .associateBy(WarmupFinalOffset::topicPartition)
                return this.assignments.map { assignment ->
                    acknowledged[assignment.destination] ?: WarmupFinalOffset(assignment.destination, null)
                }
            }

            override fun finish() = switchableDataPlane.finish(sessionId)
        }
    }

    override fun prepareRecipient(
        sessionId: WarmupSessionId,
        assignments: List<WarmupPartitionAssignment>,
        donorByDestination: Map<TopicPartition, WarmupInstanceId>,
        sendCompletion: suspend (WarmupInstanceId, WarmupRecordCompleted) -> Unit
    ): PreparedWarmupRecipient {
        require(assignments.isNotEmpty() && assignments.all { it.mapping.consumerId == consumerId }) {
            "Invalid recipient assignments for $consumerId"
        }
        val tracker = WarmupRecipientDrainTracker(sessionId)
        val outbox = WarmupCompletionOutbox(scope, completionBufferCapacity) { completed ->
            val donor = requireNotNull(donorByDestination[completed.recordId.topicPartition]) {
                "No donor owns warm-up destination ${completed.recordId.topicPartition}"
            }
            sendCompletion(donor, completed)
        }
        val observer = WarmupRecipientOutcomePublisher<K, V>(
            sessionId = sessionId,
            onPublished = tracker::onCompleted,
            publisher = outbox
        )
        val warmup = createWarmup(assignments.mapTo(linkedSetOf()) { it.destination }, observer)
        val lifecycle = WarmupRecipientLifecycle(
            createWarmup = warmup::asWarmupOwnedConsumer,
            main = main.asWarmupOwnedConsumer(),
            drainTracker = tracker,
            outbox = outbox,
            cleanupTimeout = cleanupTimeout
        )
        return object : PreparedWarmupRecipient {
            override val assignments = assignments.toList()
            override suspend fun start() = lifecycle.startWarmup()
            override suspend fun drain(finalOffsets: Collection<WarmupFinalOffset>) = lifecycle.drain(finalOffsets)
            override suspend fun startMain() {
                lifecycle.startMain()
                mainStarted.set(true)
            }
            override suspend fun stop() = lifecycle.stop()
        }
    }

    override suspend fun startMain() {
        if (mainStarted.compareAndSet(false, true)) {
            try {
                main.startRuntime()
                main.awaitRuntimeReady()
            } catch (error: Throwable) {
                runCatching { main.stopRuntime() }
                mainStarted.set(false)
                throw error
            }
        }
    }

    override suspend fun stop() {
        switchableDataPlane.close()
        if (mainStarted.get()) main.stopRuntime()
    }

    override suspend fun awaitTermination() = main.awaitRuntimeTermination()

    private fun supportsRemoteDiscard(reason: RecordDropReason): Boolean = when (reason) {
        RecordDropReason.ALREADY_PROCESSED,
        RecordDropReason.QUEUE_OVERFLOW,
        RecordDropReason.REPLACED_BY_NEWER_KEY_RECORD,
        RecordDropReason.NEW_KEY_QUEUE_FULL,
        RecordDropReason.STALE_AGE,
        RecordDropReason.LIFECYCLE_DISCARD -> true
    }
}
