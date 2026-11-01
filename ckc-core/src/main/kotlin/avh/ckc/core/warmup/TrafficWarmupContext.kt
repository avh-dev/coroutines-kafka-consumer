package avh.ckc.core.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.ManagedConsumerLifecycle
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.producer.RecordMetadata
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Explicit, non-global configuration scope shared by independently managed CKC consumers.
 * Applications do not start or stop this object: its control transport follows attached consumers.
 */
class TrafficWarmupContext internal constructor(
    internal val configuration: TrafficWarmupContextConfiguration,
    parentContext: CoroutineContext,
    channelFactory: (WarmupControlMessageHandler) -> WarmupControlChannel,
    private val metadataResolver: WarmupTopicMetadataResolver
) {
    constructor(
        configuration: TrafficWarmupContextConfiguration,
        parentContext: CoroutineContext = Dispatchers.Default
    ) : this(
        configuration,
        parentContext,
        { handler -> WarmupControlTransport(configuration.controlTransportConfiguration(), handler, parentContext = parentContext) },
        KafkaWarmupTopicMetadataResolver
    )

    private val scope = CoroutineScope(parentContext + SupervisorJob(parentContext[Job]))
    private val controlHub = SharedWarmupControlHub(scope, channelFactory)
    private val consumerIds = ConcurrentHashMap.newKeySet<WarmupConsumerId>()

    internal fun <K, V> attach(
        definition: WarmupConsumerDefinition<K, V>,
        main: CoroutinesKafkaConsumer<K, V>,
        forwarding: SwitchableWarmupRecordForwarding<K, V>,
        warmupFactory: (Set<org.apache.kafka.common.TopicPartition>) -> CoroutinesKafkaConsumer<K, V>
    ): ManagedConsumerLifecycle {
        check(consumerIds.add(definition.configuration.consumerId)) {
            "Traffic warm-up consumerId ${definition.configuration.consumerId} is already attached to this context"
        }
        return TrafficWarmupConsumerLifecycle(
            definition = definition,
            main = main,
            forwarding = forwarding,
            warmupFactory = warmupFactory
        ).also(main::attachManagedLifecycle)
    }

    private inner class TrafficWarmupConsumerLifecycle<K, V>(
        private val definition: WarmupConsumerDefinition<K, V>,
        private val main: CoroutinesKafkaConsumer<K, V>,
        private val forwarding: SwitchableWarmupRecordForwarding<K, V>,
        private val warmupFactory: (Set<org.apache.kafka.common.TopicPartition>) -> CoroutinesKafkaConsumer<K, V>
    ) : ManagedConsumerLifecycle {
        private val started = AtomicBoolean(false)
        private val stopped = AtomicBoolean(false)
        private val ready = CompletableDeferred<Unit>()
        private val termination = CompletableDeferred<Throwable?>()
        @Volatile private var coordinator: WarmupCoordinator? = null
        @Volatile private var startupJob: Job? = null

        override fun start() {
            check(!stopped.get()) { "Consumer is already stopped" }
            check(started.compareAndSet(false, true)) { "Consumer already started" }
            startupJob = scope.launch {
                try {
                    val topics = definition.configuration.topicMappings.map(WarmupTopicMapping::warmupTopic).toSet()
                    val available = try {
                        val readable = metadataResolver.resolve(
                            topics,
                            definition.warmupConsumerProperties,
                            configuration.stageTimeout
                        )
                        val writable = metadataResolver.resolve(
                            topics,
                            definition.forwardingProducerProperties,
                            configuration.stageTimeout
                        )
                        check(readable == writable) {
                            "Warm-up producer and consumer see different topic partitions"
                        }
                        readable
                    } catch (cancelled: kotlinx.coroutines.CancellationException) {
                        throw cancelled
                    } catch (_: Throwable) {
                        main.startRuntime()
                        main.awaitRuntimeReady()
                        ready.complete(Unit)
                        main.awaitRuntimeTermination()
                        termination.complete(null)
                        return@launch
                    }
                    lateinit var localCoordinator: WarmupCoordinator
                    localCoordinator = WarmupCoordinator(
                        configuration.coordinatorConfiguration(available),
                        scope.coroutineContext,
                        controlHub::channel
                    ).also { coordinator = it }
                    localCoordinator.register(
                        KafkaCoordinatorParticipant(
                            definition,
                            scope,
                            onConsumerFailure = { error -> localCoordinator.onParticipantFailure(
                                definition.configuration.consumerId, error
                            ) },
                            mainFactory = { main },
                            warmupFactory = warmupFactory,
                            forwarding = forwarding
                        )
                    )
                    localCoordinator.start()
                    ready.complete(Unit)
                    localCoordinator.awaitTermination()
                    termination.complete(null)
                } catch (error: Throwable) {
                    if (stopped.get() && error is kotlinx.coroutines.CancellationException) {
                        ready.completeExceptionally(kotlinx.coroutines.CancellationException("Consumer stopped before readiness"))
                        termination.complete(null)
                    } else {
                        ready.completeExceptionally(error)
                        termination.complete(error)
                    }
                }
            }
        }

        override suspend fun awaitReady() {
            check(started.get()) { "Consumer must be started before awaiting readiness" }
            ready.await()
        }

        override suspend fun stop() {
            if (!stopped.compareAndSet(false, true)) {
                termination.await()?.let { throw it }
                return
            }
            try {
                val activeCoordinator = coordinator
                if (activeCoordinator != null) {
                    activeCoordinator.stop()
                } else {
                    startupJob?.cancelAndJoin()
                    coordinator?.stop() ?: main.stopRuntime()
                }
                termination.complete(null)
            } catch (error: Throwable) {
                termination.complete(error)
            }
            termination.await()?.let { throw it }
        }

        override suspend fun awaitTermination() {
            termination.await()?.let { throw it }
        }
    }
}

class TrafficWarmupContextConfiguration(
    val instanceId: WarmupInstanceId,
    val controlTopic: String,
    kafkaProperties: Map<String, Any?>,
    val donorSearchTimeout: Duration = 5.seconds,
    val stageTimeout: Duration = 30.seconds,
    val heartbeatInterval: Duration = 1.seconds,
    val electionTimeout: Duration = 5.seconds,
    val donorLoadScore: () -> Double? = { null }
) {
    val kafkaProperties: Map<String, Any?> = immutableMapSnapshot(kafkaProperties)

    init {
        validateTopicName("controlTopic", controlTopic)
        require(this.kafkaProperties.isNotEmpty()) { "Traffic warm-up Kafka properties must not be empty" }
    }

    internal fun controlTransportConfiguration() = WarmupControlTransportConfiguration(
        instanceId = instanceId,
        topic = controlTopic,
        producerProperties = kafkaProperties,
        consumerProperties = kafkaProperties
    )

    internal fun coordinatorConfiguration(available: Map<String, Set<Int>>) = WarmupCoordinatorConfiguration(
        instanceId = instanceId,
        donorSearchTimeout = donorSearchTimeout,
        stageTimeout = stageTimeout,
        heartbeatInterval = heartbeatInterval,
        electionTimeout = electionTimeout,
        donorLoadScore = donorLoadScore,
        availableWarmupPartitions = available
    )
}

class TrafficWarmupContextBuilder {
    var instanceId: String? = null
    var controlTopic: String? = null
    var kafkaProperties: Map<String, Any?> = emptyMap()
    var donorSearchTimeout: Duration = 5.seconds
    var stageTimeout: Duration = 30.seconds
    var heartbeatInterval: Duration = 1.seconds
    var electionTimeout: Duration = 5.seconds
    var donorLoadScore: () -> Double? = { null }
    var parentContext: CoroutineContext = Dispatchers.Default

    internal fun build(): TrafficWarmupContext = TrafficWarmupContext(
        TrafficWarmupContextConfiguration(
            instanceId = WarmupInstanceId(requireNotNull(instanceId) { "instanceId must be specified" }),
            controlTopic = requireNotNull(controlTopic) { "controlTopic must be specified" },
            kafkaProperties = kafkaProperties,
            donorSearchTimeout = donorSearchTimeout,
            stageTimeout = stageTimeout,
            heartbeatInterval = heartbeatInterval,
            electionTimeout = electionTimeout,
            donorLoadScore = donorLoadScore
        ),
        parentContext
    )
}

fun trafficWarmupContext(block: TrafficWarmupContextBuilder.() -> Unit): TrafficWarmupContext =
    TrafficWarmupContextBuilder().apply(block).build()

internal fun interface WarmupTopicMetadataResolver {
    suspend fun resolve(
        topics: Set<String>,
        kafkaProperties: Map<String, Any?>,
        timeout: Duration
    ): Map<String, Set<Int>>
}

private object KafkaWarmupTopicMetadataResolver : WarmupTopicMetadataResolver {
    override suspend fun resolve(
        topics: Set<String>,
        kafkaProperties: Map<String, Any?>,
        timeout: Duration
    ): Map<String, Set<Int>> = withContext(Dispatchers.IO) {
        Admin.create(kafkaProperties).use { admin ->
            val descriptions = admin.describeTopics(topics)
                .allTopicNames()
                .get(timeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
            topics.associateWith { topic ->
                descriptions.getValue(topic).partitions().map { it.partition() }.toSet().also { partitions ->
                    check(partitions.isNotEmpty()) { "Warm-up topic $topic has no partitions" }
                }
            }
        }
    }
}

/** One physical control transport multiplexed to consumer-scoped coordinator engines. */
internal class SharedWarmupControlHub(
    private val scope: CoroutineScope,
    private val transportFactory: (WarmupControlMessageHandler) -> WarmupControlChannel
) {
    private val mutex = Mutex()
    private val handlers = ConcurrentHashMap<String, WarmupControlMessageHandler>()
    private val lastRecordOffset = AtomicLong(-1)
    private var transport: WarmupControlChannel? = null

    fun channel(handler: WarmupControlMessageHandler): WarmupControlChannel = HubChannel(
        UUID.randomUUID().toString(), handler
    )

    private suspend fun acquire(id: String, handler: WarmupControlMessageHandler): HubLease = mutex.withLock {
        handlers[id] = handler
        try {
            val current = transport ?: transportFactory(WarmupControlMessageHandler { record ->
                lastRecordOffset.set(record.offset)
                handlers.values.toList().forEach { it.onMessage(record) }
            }).also { created ->
                try {
                    created.start()
                    created.awaitReady()
                    lastRecordOffset.set(created.logPosition().endOffset - 1)
                    transport = created
                } catch (error: Throwable) {
                    runCatching { created.stop() }
                    throw error
                }
            }
            val base = current.logPosition()
            HubLease(current, base.copy(endOffset = lastRecordOffset.get() + 1))
        } catch (error: Throwable) {
            handlers.remove(id)
            throw error
        }
    }

    private suspend fun release(id: String, lease: HubLease) = mutex.withLock {
        handlers.remove(id)
        if (handlers.isEmpty() && transport === lease.transport) {
            transport = null
            lease.transport.stop()
            lastRecordOffset.set(-1)
        }
    }

    private inner class HubChannel(
        private val id: String,
        private val handler: WarmupControlMessageHandler
    ) : WarmupControlChannel {
        private val started = AtomicBoolean(false)
        private val stopped = CompletableDeferred<Unit>()
        private val lease = CompletableDeferred<HubLease>()
        @Volatile private var position: WarmupControlLogPosition? = null

        override fun start() {
            check(started.compareAndSet(false, true)) { "Warm-up control channel already started" }
            scope.launch {
                try {
                    val acquired = acquire(id, handler)
                    position = acquired.position
                    lease.complete(acquired)
                    if (stopped.isCompleted) release(id, acquired)
                } catch (error: Throwable) {
                    lease.completeExceptionally(error)
                }
            }
        }

        override suspend fun awaitReady() {
            lease.await()
        }

        override fun logPosition(): WarmupControlLogPosition =
            checkNotNull(position) { "Warm-up control channel is not ready" }

        override suspend fun send(message: WarmupControlMessage): RecordMetadata =
            lease.await().transport.send(message)

        override suspend fun awaitTermination() = coroutineScope {
            val underlying = async { lease.await().transport.awaitTermination() }
            try {
                select<Unit> {
                    stopped.onAwait { }
                    underlying.onAwait { }
                }
            } finally {
                underlying.cancel()
            }
        }

        override suspend fun stop() {
            if (!stopped.complete(Unit)) return
            if (lease.isCompleted) runCatching { release(id, lease.await()) }
        }
    }

    private data class HubLease(
        val transport: WarmupControlChannel,
        val position: WarmupControlLogPosition
    )
}
