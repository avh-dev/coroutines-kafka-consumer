package avh.ckc.core.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.CoroutinesKafkaConsumerBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.common.TopicPartition

/**
 * One application definition reused by core to create either the main or explicitly assigned
 * warm-up consumer. The application supplies one handler/runtime configuration, not two consumer
 * lifecycles.
 */
internal class WarmupConsumerDefinition<K, V>(
    val configuration: WarmupParticipantConfiguration,
    mainConsumerProperties: Map<String, Any?>,
    warmupConsumerProperties: Map<String, Any?> = mainConsumerProperties,
    forwardingProducerProperties: Map<String, Any?>,
    val forwardingObserver: (WarmupSessionId) -> WarmupForwardingObserver = { WarmupForwardingObserver.NOOP },
    val configure: CoroutinesKafkaConsumerBuilder<K, V>.() -> Unit
) {
    val mainConsumerProperties: Map<String, Any?> = immutableMapSnapshot(mainConsumerProperties)
    val warmupConsumerProperties: Map<String, Any?> = immutableMapSnapshot(warmupConsumerProperties)
    val forwardingProducerProperties: Map<String, Any?> = immutableMapSnapshot(forwardingProducerProperties)
}

internal interface CoordinatorParticipant {
    val configuration: WarmupParticipantConfiguration
    suspend fun startMain()
    fun capability(requested: WarmupParticipantConfiguration): WarmupParticipantCapability
    suspend fun reserveDonor(assignment: List<WarmupPartitionAssignment>)
    suspend fun releaseDonorReservation()
    suspend fun activateDonor(sessionId: WarmupSessionId)
    suspend fun completeForwarding(): WarmupForwardingBoundary
    suspend fun finishDonor(sessionId: WarmupSessionId)
    suspend fun startRecipient(assignment: List<WarmupPartitionAssignment>)
    suspend fun drainRecipient(boundary: WarmupForwardingBoundary)
    suspend fun startMainAfterDrain()
    suspend fun prepareFallback(knownFailure: Throwable)
    suspend fun stop()
}

internal class KafkaCoordinatorParticipant<K, V>(
    private val definition: WarmupConsumerDefinition<K, V>,
    private val scope: CoroutineScope,
    private val onConsumerFailure: suspend (Throwable) -> Unit,
    private val mainFactory: (() -> CoroutinesKafkaConsumer<K, V>)? = null,
    private val warmupFactory: ((Set<TopicPartition>) -> CoroutinesKafkaConsumer<K, V>)? = null,
    private val forwarding: SwitchableWarmupRecordForwarding<K, V> = SwitchableWarmupRecordForwarding()
) : CoordinatorParticipant {
    override val configuration: WarmupParticipantConfiguration = definition.configuration
    private val mutex = Mutex()
    private var main: CoroutinesKafkaConsumer<K, V>? = null
    private var recipient: WarmupRecipientLifecycle<K, V>? = null
    private var donorAssignment: List<WarmupPartitionAssignment>? = null

    override suspend fun startMain() = mutex.withLock {
        check(main == null && recipient == null) { "Consumer ${configuration.consumerId} already started" }
        buildMain().also { main = it }.startRuntime()
        requireNotNull(main).awaitRuntimeReady()
        observeFailure(requireNotNull(main))
    }

    override fun capability(requested: WarmupParticipantConfiguration): WarmupParticipantCapability {
        val current = main
        val localMappings = configuration.topicMappings.associateBy(WarmupTopicMapping::sourceTopic)
        val snapshot = current?.stateSnapshot()
        val assignedByTopic = snapshot?.pollLoops.orEmpty()
            .flatMap { it.assignedPartitions }
            .groupBy({ it.topic }, { it.partition })
        return WarmupParticipantCapability(
            configuration.consumerId,
            requested.topicMappings.map { requestedMapping ->
                val compatible = localMappings[requestedMapping.sourceTopic] == requestedMapping
                val owned = if (compatible) assignedByTopic[requestedMapping.sourceTopic].orEmpty() else emptyList()
                WarmupTopicCapability(
                    sourceTopic = requestedMapping.sourceTopic,
                    compatible = compatible,
                    ownedPartitions = owned,
                    observedMessages = if (compatible) snapshot?.pollLoops.orEmpty().fold(0L) { total, loop ->
                        val observed = loop.observedRecordsByTopic[requestedMapping.sourceTopic] ?: 0L
                        if (Long.MAX_VALUE - total < observed) Long.MAX_VALUE else total + observed
                    } else 0
                )
            }
        )
    }

    override suspend fun reserveDonor(assignment: List<WarmupPartitionAssignment>) = mutex.withLock {
        check(donorAssignment == null) { "Consumer ${configuration.consumerId} is already reserved" }
        check(assignment.isNotEmpty() && assignment.all { it.consumerId == configuration.consumerId }) {
            "Invalid donor assignment for ${configuration.consumerId}"
        }
        check(main != null) { "Donor main consumer is not running" }
        donorAssignment = immutableListSnapshot(assignment)
    }

    override suspend fun releaseDonorReservation() = mutex.withLock {
        donorAssignment = null
    }

    override suspend fun activateDonor(sessionId: WarmupSessionId) = mutex.withLock {
        val assignment = requireNotNull(donorAssignment) { "Donor is not reserved" }
        forwarding.activate(
            WarmupForwardingConfiguration(
                sessionId = sessionId,
                consumerId = configuration.consumerId,
                assignments = assignment,
                profile = configuration.forwardingProfile,
                producerProperties = definition.forwardingProducerProperties
            ),
            definition.forwardingObserver(sessionId)
        )
    }

    override suspend fun completeForwarding(): WarmupForwardingBoundary = mutex.withLock {
        requireNotNull(main) { "Donor main consumer is not running" }.completeWarmupForwarding()
    }

    override suspend fun finishDonor(sessionId: WarmupSessionId) = mutex.withLock {
        forwarding.finish(sessionId)
        donorAssignment = null
    }

    override suspend fun startRecipient(assignment: List<WarmupPartitionAssignment>) {
        val lifecycle = mutex.withLock {
            check(assignment.isNotEmpty() && assignment.all { it.consumerId == configuration.consumerId }) {
                "Invalid recipient assignment for ${configuration.consumerId}"
            }
            check(recipient == null) { "Recipient ${configuration.consumerId} is already started" }
            val partitions = assignment.map(WarmupPartitionAssignment::topicPartition).toSet()
            WarmupRecipientLifecycle(
                createWarmup = { buildWarmup(partitions) },
                createMain = { buildMain() }
            ).also { recipient = it }
        }
        lifecycle.startWarmup()
        observeFailure(lifecycle.warmupConsumer())
    }

    override suspend fun drainRecipient(boundary: WarmupForwardingBoundary) {
        requireNotNull(recipient) { "Recipient is not running" }.drain(boundary)
    }

    override suspend fun startMainAfterDrain() = mutex.withLock {
        val lifecycle = requireNotNull(recipient) { "Recipient is not running" }
        lifecycle.startMain()
        main = lifecycle.mainConsumer()
        observeFailure(requireNotNull(main))
    }

    override suspend fun prepareFallback(knownFailure: Throwable) {
        val lifecycle = mutex.withLock { recipient.also { recipient = null } }
        lifecycle?.stop(knownFailure)
    }

    override suspend fun stop() = mutex.withLock {
        recipient?.stop() ?: main?.stopRuntime()
        forwarding.close()
    }

    private fun buildMain(): CoroutinesKafkaConsumer<K, V> =
        mainFactory?.invoke() ?: CoroutinesKafkaConsumerBuilder<K, V>()
            .apply(definition.configure)
            .apply {
                topics(configuration.topicMappings.map(WarmupTopicMapping::sourceTopic))
                warmupForwarding(forwarding)
            }
            .build(definition.mainConsumerProperties)

    private fun buildWarmup(partitions: Set<TopicPartition>): CoroutinesKafkaConsumer<K, V> =
        warmupFactory?.invoke(partitions) ?: CoroutinesKafkaConsumerBuilder<K, V>()
            .apply(definition.configure)
            .apply { assignedPartitionsAtEnd(partitions) }
            .build(definition.warmupConsumerProperties)

    private fun observeFailure(consumer: CoroutinesKafkaConsumer<K, V>) {
        scope.launch {
            try {
                consumer.awaitRuntimeTermination()
            } catch (error: Throwable) {
                onConsumerFailure(error)
            }
        }
    }
}
