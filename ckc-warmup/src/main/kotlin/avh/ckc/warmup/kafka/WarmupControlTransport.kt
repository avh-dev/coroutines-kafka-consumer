package avh.ckc.warmup.kafka

import avh.ckc.core.isFatalJvmError
import avh.ckc.warmup.protocol.WarmupControlCodec
import avh.ckc.warmup.protocol.WarmupControlMessage
import avh.ckc.warmup.protocol.WarmupControlMessageHandler
import avh.ckc.warmup.protocol.WarmupControlNamespace
import avh.ckc.warmup.protocol.WarmupControlRecord
import avh.ckc.warmup.protocol.WarmupInstanceId
import java.time.Duration as JavaDuration
import java.util.Collections
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.DescribeTopicsOptions
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.TimeoutException as KafkaTimeoutException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

/** Resolved client settings and timing budgets for one control transport. */
internal class WarmupControlTransportConfiguration(
    val instanceId: WarmupInstanceId,
    val namespace: WarmupControlNamespace,
    val topic: String,
    producerProperties: Map<String, Any?>,
    consumerProperties: Map<String, Any?>,
    val groupIdPrefix: String = "ckc.traffic-warmup.v6.control",
    val pollTimeout: Duration = 100.milliseconds,
    val sendTimeout: Duration = 30.seconds,
    val metadataTimeout: Duration = 30.seconds,
    val assignmentTimeout: Duration = 15.seconds,
    val topicIdentityCheckInterval: Duration = 5.seconds,
    val recoveryRetryBackoff: Duration = 500.milliseconds,
    val commandBufferCapacity: Int = 1_024,
    val preserveSendOrder: Boolean = false
) {
    val producerProperties: Map<String, Any?> = immutableProperties(producerProperties)
    val consumerProperties: Map<String, Any?> = immutableProperties(consumerProperties)
    val groupId = "$groupIdPrefix-${namespace.value}-${instanceId.value}"

    init {
        require(topic.isNotBlank() && topic.length <= 249 && topic.none(Char::isISOControl)) {
            "Control topic must be a valid Kafka topic name"
        }
        require(groupIdPrefix.isNotBlank()) { "Control group id prefix must not be blank" }
        require(pollTimeout.isPositive() && pollTimeout.inWholeMilliseconds > 0) {
            "Control poll timeout must be at least one millisecond"
        }
        require(sendTimeout.isPositive() && sendTimeout.inWholeMilliseconds > 0) {
            "Control send timeout must be at least one millisecond"
        }
        require(metadataTimeout.isPositive() && metadataTimeout.inWholeMilliseconds > 0) {
            "Control metadata timeout must be at least one millisecond"
        }
        require(assignmentTimeout.isPositive() && assignmentTimeout.inWholeMilliseconds > 0) {
            "Control assignment timeout must be at least one millisecond"
        }
        require(topicIdentityCheckInterval.isPositive()) { "Control topic identity check interval must be positive" }
        require(recoveryRetryBackoff.isPositive()) { "Control recovery retry backoff must be positive" }
        require(commandBufferCapacity > 0) { "Control command buffer capacity must be positive" }
        require(this.producerProperties.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            "Control producer bootstrap servers are required"
        }
        require(this.consumerProperties.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            "Control consumer bootstrap servers are required"
        }
    }
}

/** Handles a malformed control record on the dispatch coroutine after its offset has advanced. */
internal fun interface WarmupInvalidControlRecordHandler {
    fun onInvalidRecord(key: String?, error: Throwable)

    companion object {
        val FAIL = WarmupInvalidControlRecordHandler { key, error ->
            throw InvalidWarmupControlRecordException(key, error)
        }
    }
}

/** Terminates dispatch when the configured invalid-record policy rejects a record. */
internal class InvalidWarmupControlRecordException(key: String?, cause: Throwable) :
    IllegalArgumentException("Invalid warm-up control record with key ${key ?: "<null>"}", cause)

/** Distinguishes the transport send deadline from cancellation by a caller. */
internal class WarmupControlSendTimeoutException(timeout: Duration) :
    IllegalStateException("Warm-up control send exceeded $timeout")

/** Lifecycle and record transport used by the warm-up discovery runtime. */
internal interface WarmupControlChannel {
    fun start()
    suspend fun awaitReady()
    suspend fun send(message: WarmupControlMessage): RecordMetadata
    fun initialOffsetBoundary(): Long?
    suspend fun awaitTermination()
    suspend fun stop()
}

/** Starts at the current end of the one-partition control topic and never replays protocol history. */
internal class WarmupControlTransport(
    private val configuration: WarmupControlTransportConfiguration,
    private val handler: WarmupControlMessageHandler,
    private val invalidRecordHandler: WarmupInvalidControlRecordHandler = WarmupInvalidControlRecordHandler.FAIL,
    parentContext: CoroutineContext = Dispatchers.IO,
    private val producerFactory: (Map<String, Any?>) -> Producer<String, ByteArray> = { KafkaProducer(it) },
    private val consumerFactory: (Map<String, Any?>) -> Consumer<String, ByteArray> = { KafkaConsumer(it) },
    private val topicIdentity: WarmupControlTopicIdentity = KafkaWarmupControlTopicIdentity(
        configuration.consumerProperties,
        configuration.metadataTimeout
    ),
    private val dispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val nanoTime: () -> Long = System::nanoTime
) : WarmupControlChannel {
    private sealed interface DispatchItem {
        data class Record(val record: WarmupControlRecord) : DispatchItem
        data class Invalid(val key: String?, val error: Throwable) : DispatchItem
    }

    private val lifecycleJob = SupervisorJob(parentContext[Job])
    private val log = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(parentContext.minusKey(Job) + lifecycleJob + CoroutineName("WarmupControl"))
    private val shutdownScope = CoroutineScope(
        parentContext.minusKey(Job) + SupervisorJob() + CoroutineName("WarmupControlShutdown")
    )
    private val commands = Channel<DispatchItem>(configuration.commandBufferCapacity)
    private val readiness = CompletableDeferred<Unit>()
    private val termination = CompletableDeferred<Throwable?>()
    private val failure = AtomicReference<Throwable?>()
    private val started = AtomicBoolean(false)
    private val stopping = AtomicBoolean(false)
    private val shutdownJob = AtomicReference<Job?>()
    @Volatile private var producer: Producer<String, ByteArray>? = null
    @Volatile private var consumer: Consumer<String, ByteArray>? = null
    private var pollJob: Job? = null
    private var dispatchJob: Job? = null
    @Volatile private var initialBoundary: Long? = null

    override fun initialOffsetBoundary(): Long? = initialBoundary

    override fun start() {
        check(started.compareAndSet(false, true)) { "Warm-up control transport already started" }
        dispatchJob = scope.launch { dispatchLoop() }.also(::observe)
        pollJob = scope.launch(dispatcher) {
            try {
                producer = producerFactory(producerProperties())
                consumer = consumerFactory(consumerProperties())
                pollLoop()
            } catch (error: Throwable) {
                if (error !is CancellationException || !stopping.get()) fail(error)
                withContext(NonCancellable + dispatcher) { closeClients() }
                throw error
            }
        }.also(::observe)
    }

    override suspend fun awaitReady() {
        check(started.get()) { "Warm-up control transport has not started" }
        readiness.await()
        failure.get()?.let { throw it }
    }

    override suspend fun send(message: WarmupControlMessage): RecordMetadata {
        check(message.senderInstanceId == configuration.instanceId) {
            "Control message sender must match the local instance"
        }
        check(message.namespace == configuration.namespace) {
            "Control message namespace must match the configured namespace"
        }
        awaitReady()
        check(!stopping.get()) { "Warm-up control transport is stopping" }
        failure.get()?.let { throw it }
        val record = WarmupControlCodec.producerRecord(configuration.topic, message)
        return withTimeoutOrNull(configuration.sendTimeout) {
            val acknowledgement = CompletableDeferred<RecordMetadata>()
            runInterruptible(dispatcher) {
                requireNotNull(producer).send(record) { metadata, error ->
                    when {
                        error != null -> acknowledgement.completeExceptionally(error)
                        metadata == null -> acknowledgement.completeExceptionally(
                            IllegalStateException("Control producer acknowledgement did not contain metadata")
                        )
                        else -> acknowledgement.complete(metadata)
                    }
                }
            }
            acknowledgement.await()
        } ?: throw WarmupControlSendTimeoutException(configuration.sendTimeout)
    }

    override suspend fun awaitTermination() {
        termination.await()?.let { throw it }
    }

    override suspend fun stop() {
        shutdownOperation().join()
    }

    private fun shutdownOperation(): Job {
        shutdownJob.get()?.let { return it }
        val created = shutdownScope.launch(start = CoroutineStart.LAZY) { shutdown() }
        if (shutdownJob.compareAndSet(null, created)) {
            created.start()
            return created
        }
        created.cancel()
        return requireNotNull(shutdownJob.get())
    }

    private suspend fun shutdown() {
        stopping.set(true)
        consumer?.wakeup()
        pollJob?.cancelAndJoin()
        commands.close()
        dispatchJob?.cancelAndJoin()
        withContext(NonCancellable + dispatcher) { closeClients() }
        if (!readiness.isCompleted) {
            readiness.completeExceptionally(CancellationException("Control transport stopped before readiness"))
        }
        termination.complete(failure.get())
        scope.cancel()
    }

    private suspend fun pollLoop() {
        var activeConsumer = requireNotNull(consumer)
        val read = WarmupControlReadState()
        while (currentCoroutineContext().isActive && !stopping.get()) {
            try {
                pollControlTopic(activeConsumer, read)
                return
            } catch (recovery: WarmupControlRecoveryRequiredException) {
                if (stopping.get()) return
                when (recovery) {
                    is WarmupControlTopicRecreatedException -> {
                        read.expectedTopicIdentity = recovery.currentIdentity
                        read.nextOffset = null
                        consumer = null
                        replaceProducerAfterTopicRecreation()
                    }
                    else -> {
                        log.warn("Warm-up control consumer requires a bounded replacement", recovery)
                        consumer = null
                    }
                }
                delay(configuration.recoveryRetryBackoff)
                activeConsumer = consumerFactory(consumerProperties()).also { consumer = it }
            }
        }
    }

    private suspend fun pollControlTopic(
        activeConsumer: Consumer<String, ByteArray>,
        read: WarmupControlReadState
    ) {
        try {
            subscribe(activeConsumer, read)
            while (currentCoroutineContext().isActive && !stopping.get()) {
                val records = pollControlRecords(activeConsumer) ?: break
                checkAssignmentCallback(read)
                dispatchReadRecords(read, records)
                checkAssignmentAndSentinel(read)
            }
        } finally {
            (read.assignment as? WarmupControlAssignmentState.Active)?.identityCheck?.cancel()
            withContext(NonCancellable + dispatcher) {
                runCatching { activeConsumer.close(closeDuration()) }
            }
            read.assignment = null
        }
    }

    private fun subscribe(activeConsumer: Consumer<String, ByteArray>, read: WarmupControlReadState) {
        val metadata = activeConsumer.partitionsFor(
            configuration.topic,
            JavaDuration.ofMillis(configuration.metadataTimeout.inWholeMilliseconds)
        )
        check(metadata.size == 1 && metadata.single().partition() == 0) {
            "Warm-up control topic must expose exactly partition 0"
        }
        read.assignment = awaitingAssignment()
        activeConsumer.subscribe(listOf(configuration.topic), object : ConsumerRebalanceListener {
            override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
                (read.assignment as? WarmupControlAssignmentState.Active)?.identityCheck?.cancel()
                read.assignment = awaitingAssignment()
            }

            override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                try {
                    initializeAssignedPartition(activeConsumer, read, partitions)
                } catch (error: Throwable) {
                    if (error.isFatalJvmError()) throw error
                    // Kafka can wrap errors escaping the listener. The poll loop sees this cause directly.
                    read.assignment = WarmupControlAssignmentState.CallbackFailed(error)
                }
            }
        })
    }

    private fun initializeAssignedPartition(
        activeConsumer: Consumer<String, ByteArray>,
        read: WarmupControlReadState,
        partitions: Collection<TopicPartition>
    ) {
        check(partitions.size == 1) { "Warm-up control topic must have exactly one partition" }
        val partition = partitions.single()
        check(partition.topic() == configuration.topic && partition.partition() == 0) {
            "Warm-up control topic must use partition 0"
        }
        val awaiting = read.assignment as? WarmupControlAssignmentState.Awaiting ?: awaitingAssignment()
        val currentTopicIdentity = withinAssignmentDeadline(
            awaiting,
            "checking the assigned control topic identity"
        ) { timeout ->
            topicIdentity.read(configuration.topic, timeout.toMillis().milliseconds)
        }
        if (!readiness.isCompleted) {
            read.expectedTopicIdentity = currentTopicIdentity
            val end = withinAssignmentDeadline(awaiting, "reading the initial log end") { timeout ->
                activeConsumer.endOffsets(partitions.toSet(), timeout).getValue(partition)
            }
            activeConsumer.seek(partition, end)
            withinAssignmentDeadline(awaiting, "initializing the initial position") { timeout ->
                activeConsumer.position(partition, timeout)
            }
            read.nextOffset = end
            initialBoundary = end - 1
            readiness.complete(Unit)
        } else {
            val expectedIdentity = requireNotNull(read.expectedTopicIdentity)
            if (currentTopicIdentity != expectedIdentity) {
                read.assignment = WarmupControlAssignmentState.TopicRecreated(expectedIdentity, currentTopicIdentity)
                return
            }
            // Resume the cursor only on the same topic ID. A new generation starts with no cursor.
            read.nextOffset?.let { activeConsumer.seek(partition, it) }
            withinAssignmentDeadline(awaiting, "initializing the replacement position") { timeout ->
                activeConsumer.position(partition, timeout).also { read.nextOffset = it }
            }
        }
        read.assignment = activeAssignment()
    }

    private fun pollControlRecords(activeConsumer: Consumer<String, ByteArray>): ConsumerRecords<String, ByteArray>? =
        try {
            activeConsumer.poll(JavaDuration.ofMillis(configuration.pollTimeout.inWholeMilliseconds))
        } catch (wakeup: WakeupException) {
            if (stopping.get()) null else throw wakeup
        } catch (error: Exception) {
            when (val pollFailure = classifyPollFailure(error)) {
                is WarmupControlPollFailure.Temporary -> {
                    log.warn(
                        "Warm-up control poll is temporarily unavailable; keeping the current consumer " +
                            "and continuing to poll (reason={})",
                        pollFailure.reason,
                        error
                    )
                    ConsumerRecords.empty()
                }
                is WarmupControlPollFailure.Terminal -> throw error
            }
        }

    private fun checkAssignmentCallback(read: WarmupControlReadState) {
        when (val assignment = requireNotNull(read.assignment)) {
            is WarmupControlAssignmentState.TopicRecreated ->
                throw WarmupControlTopicRecreatedException(assignment.previousIdentity, assignment.currentIdentity)
            is WarmupControlAssignmentState.CallbackFailed -> throw assignment.error
            is WarmupControlAssignmentState.Active, is WarmupControlAssignmentState.Awaiting -> Unit
        }
    }

    private fun dispatchReadRecords(
        read: WarmupControlReadState,
        records: ConsumerRecords<String, ByteArray>
    ) {
        records.forEach { record ->
            read.nextOffset = maxOf(read.nextOffset ?: 0L, record.offset() + 1)
            val message = try {
                WarmupControlCodec.decode(record.value(), record.headers()).also { decoded ->
                    require(record.key() == decoded.sessionId.value) {
                        "Control record key does not match its session"
                    }
                }
            } catch (error: Exception) {
                enqueue(DispatchItem.Invalid(record.key(), error))
                return@forEach
            }
            if (message.namespace == configuration.namespace) {
                enqueue(DispatchItem.Record(WarmupControlRecord(record.offset(), message)))
            }
        }
    }

    private suspend fun checkAssignmentAndSentinel(read: WarmupControlReadState) {
        val now = nanoTime()
        when (val assignment = requireNotNull(read.assignment)) {
            is WarmupControlAssignmentState.Awaiting -> {
                if (now >= assignment.deadlineNanos) {
                    throw WarmupControlAssignmentTimeoutException(configuration.assignmentTimeout)
                }
            }
            is WarmupControlAssignmentState.Active -> checkSentinel(read, assignment, now)
            is WarmupControlAssignmentState.TopicRecreated -> error("Topic recreation must be handled first")
            is WarmupControlAssignmentState.CallbackFailed -> error("Callback failure must be handled first")
        }
    }

    private suspend fun checkSentinel(
        read: WarmupControlReadState,
        assignment: WarmupControlAssignmentState.Active,
        now: Long
    ) {
        assignment.identityCheck?.takeIf { it.isCompleted }?.let {
            assignment.identityCheck = null
            when (val check = it.await()) {
                WarmupControlTopicIdentityCheck.Unavailable -> Unit
                is WarmupControlTopicIdentityCheck.Terminal -> throw check.error
                is WarmupControlTopicIdentityCheck.Available -> {
                    val expected = requireNotNull(read.expectedTopicIdentity)
                    if (check.identity != expected) {
                        throw WarmupControlTopicRecreatedException(expected, check.identity)
                    }
                }
            }
            assignment.nextIdentityCheckNanos = now + configuration.topicIdentityCheckInterval.inWholeNanoseconds
        }
        if (assignment.identityCheck == null && now >= assignment.nextIdentityCheckNanos) {
            assignment.identityCheck = scope.async { checkTopicIdentity() }
        }
    }

    private fun awaitingAssignment() = WarmupControlAssignmentState.Awaiting(
        nanoTime() + configuration.assignmentTimeout.inWholeNanoseconds
    )

    private fun activeAssignment(): WarmupControlAssignmentState = WarmupControlAssignmentState.Active(
        nanoTime() + configuration.topicIdentityCheckInterval.inWholeNanoseconds
    )

    private suspend fun checkTopicIdentity(): WarmupControlTopicIdentityCheck = try {
        val timeout = minOf(configuration.metadataTimeout, configuration.topicIdentityCheckInterval)
        val identity = runInterruptible(dispatcher) { topicIdentity.read(configuration.topic, timeout) }
        WarmupControlTopicIdentityCheck.Available(identity)
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (error: Throwable) {
        if (error.isFatalJvmError()) throw error
        val cause = unwrapKafkaMetadataFailure(error)
        when (cause) {
            is KafkaTimeoutException, is java.util.concurrent.TimeoutException, is RetriableException ->
                WarmupControlTopicIdentityCheck.Unavailable
            else -> WarmupControlTopicIdentityCheck.Terminal(cause)
        }
    }

    private fun <T> withinAssignmentDeadline(
        awaiting: WarmupControlAssignmentState.Awaiting,
        operation: String,
        block: (JavaDuration) -> T
    ): T {
        val remainingNanos = awaiting.deadlineNanos - nanoTime()
        if (remainingNanos <= 0) {
            throw WarmupControlAssignmentTimeoutException(configuration.assignmentTimeout, operation)
        }
        return try {
            val result = block(JavaDuration.ofNanos(remainingNanos))
            if (nanoTime() >= awaiting.deadlineNanos) {
                throw WarmupControlAssignmentTimeoutException(configuration.assignmentTimeout, operation)
            }
            result
        } catch (error: Throwable) {
            if (error.isFatalJvmError()) throw error
            val cause = unwrapKafkaMetadataFailure(error)
            if (cause is KafkaTimeoutException ||
                cause is java.util.concurrent.TimeoutException ||
                cause is RetriableException
            ) {
                throw WarmupControlMetadataUnavailableException(operation, cause)
            }
            throw cause
        }
    }

    private suspend fun dispatchLoop() {
        for (item in commands) {
            when (item) {
                is DispatchItem.Record -> handler.onMessage(item.record)
                is DispatchItem.Invalid -> invalidRecordHandler.onInvalidRecord(item.key, item.error)
            }
        }
    }

    private fun enqueue(item: DispatchItem) {
        check(commands.trySend(item).isSuccess) { "Warm-up control command buffer is full or closed" }
    }

    private fun observe(job: Job) {
        job.invokeOnCompletion { error ->
            if (error != null && error !is CancellationException && !stopping.get()) fail(error)
        }
    }

    private fun fail(error: Throwable) {
        if (failure.compareAndSet(null, error)) {
            readiness.completeExceptionally(error)
            termination.complete(error)
            commands.close(error)
            consumer?.wakeup()
            pollJob?.cancel(CancellationException("Warm-up control transport failed", error))
            dispatchJob?.cancel(CancellationException("Warm-up control transport failed", error))
        }
    }

    private fun closeClients() {
        runCatching { producer?.close(closeDuration()) }
        runCatching { consumer?.close(closeDuration()) }
        runCatching { topicIdentity.close() }
    }

    private fun replaceProducerAfterTopicRecreation() {
        val replacement = producerFactory(producerProperties())
        val previous = producer
        producer = replacement
        runCatching { previous?.close(closeDuration()) }
    }

    private fun closeDuration() = JavaDuration.ofMillis(configuration.sendTimeout.inWholeMilliseconds)

    private fun producerProperties(): Map<String, Any?> {
        val properties = buildControlProducerProperties(
            configuration.producerProperties,
            mapOf(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                ProducerConfig.ACKS_CONFIG to "all",
                ProducerConfig.CLIENT_ID_CONFIG to "${configuration.groupId}-producer",
                ProducerConfig.MAX_BLOCK_MS_CONFIG to configuration.sendTimeout.inWholeMilliseconds
            )
        )
        if (!configuration.preserveSendOrder) return properties
        val idempotenceDisabled = properties[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG]
            ?.toString()?.trim().equals("false", ignoreCase = true)
        val inflight = properties[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION]
            ?.toString()?.trim()?.toIntOrNull()
        // A cancelled ACK wait leaves Kafka's send alive. Its retry must precede a later SessionEnded.
        return if (idempotenceDisabled || (inflight != null && inflight > 5)) {
            properties + (ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to 1)
        } else properties
    }

    private fun consumerProperties(): Map<String, Any?> = buildControlConsumerProperties(
        configuration.consumerProperties,
        mapOf(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
            ConsumerConfig.GROUP_ID_CONFIG to configuration.groupId,
            ConsumerConfig.CLIENT_ID_CONFIG to "${configuration.groupId}-consumer",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        )
    )
}

/** Moves the poll loop through its bounded Kafka client replacement path. */
private open class WarmupControlRecoveryRequiredException(
    message: String
) : IllegalStateException(message)

/** Indicates that an assignment did not establish a usable position in time. */
private class WarmupControlAssignmentTimeoutException(
    timeout: Duration,
    operation: String = "waiting for assignment",
    cause: Throwable? = null
) :
    WarmupControlRecoveryRequiredException(
        "Warm-up control consumer exceeded the $timeout assignment deadline while $operation"
    ) {
    init {
        if (cause != null) initCause(cause)
    }
}

/** Preserves a retryable assignment metadata failure across the Kafka callback. */
private class WarmupControlMetadataUnavailableException(operation: String, cause: Throwable) :
    WarmupControlRecoveryRequiredException(
        "Warm-up control metadata is temporarily unavailable while $operation"
    ) {
    init {
        initCause(cause)
    }
}

/** Assignment deadlines apply only while Kafka has no usable control-topic position. */
private sealed interface WarmupControlAssignmentState {
    data class Awaiting(val deadlineNanos: Long) : WarmupControlAssignmentState
    data class Active(
        var nextIdentityCheckNanos: Long,
        var identityCheck: Deferred<WarmupControlTopicIdentityCheck>? = null
    ) : WarmupControlAssignmentState
    data class TopicRecreated(
        val previousIdentity: String,
        val currentIdentity: String
    ) : WarmupControlAssignmentState
    data class CallbackFailed(val error: Throwable) : WarmupControlAssignmentState
}

/** Result of a periodic metadata sentinel for the assigned control topic. */
private sealed interface WarmupControlTopicIdentityCheck {
    data class Available(val identity: String) : WarmupControlTopicIdentityCheck
    data class Terminal(val error: Throwable) : WarmupControlTopicIdentityCheck
    data object Unavailable : WarmupControlTopicIdentityCheck
}

/** Classifies failures for transport diagnostics and consumer replacement decisions. */
private enum class WarmupControlPollFailureReason {
    TIMEOUT,
    RETRIABLE,
    AUTHENTICATION,
    AUTHORIZATION,
    OTHER
}

/** Reads a Kafka topic identity through metadata independently of consumer polling. */
internal interface WarmupControlTopicIdentity : AutoCloseable {
    fun read(topic: String, timeout: Duration): String
    override fun close() = Unit
}

/**
 * Poll-loop state carried across control consumer replacements. Assignment and its pending topic
 * identity sentinel belong to the current consumer. The cursor is valid for one topic ID.
 */
private class WarmupControlReadState {
    var assignment: WarmupControlAssignmentState? = null
    var nextOffset: Long? = null
    var expectedTopicIdentity: String? = null
}

/** Uses a dedicated Admin client for assignment and sentinel topic ID checks. */
internal class KafkaWarmupControlTopicIdentity(
    properties: Map<String, Any?>,
    private val closeTimeout: Duration
) : WarmupControlTopicIdentity {
    private val admin = Admin.create(properties)

    override fun read(topic: String, timeout: Duration): String {
        val timeoutMillis = timeout.inWholeMilliseconds.coerceIn(1, Int.MAX_VALUE.toLong())
        return admin.describeTopics(listOf(topic), DescribeTopicsOptions().timeoutMs(timeoutMillis.toInt()))
            .allTopicNames()
            .get(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
            .getValue(topic)
            .topicId()
            .toString()
    }

    override fun close() = admin.close(
        JavaDuration.ofMillis(closeTimeout.inWholeMilliseconds.coerceAtLeast(1))
    )
}

/** Carries the topic ID that invalidates the previous cursor and producer. */
private class WarmupControlTopicRecreatedException(
    previousIdentity: String,
    val currentIdentity: String
) : WarmupControlRecoveryRequiredException(
    "Warm-up control topic identity changed from $previousIdentity to $currentIdentity"
)

/** Separates transient poll errors that retain the consumer from terminal poll failures. */
private sealed interface WarmupControlPollFailure {
    val reason: WarmupControlPollFailureReason

    data class Temporary(override val reason: WarmupControlPollFailureReason) : WarmupControlPollFailure
    data class Terminal(override val reason: WarmupControlPollFailureReason) : WarmupControlPollFailure
}

private fun classifyPollFailure(error: Throwable): WarmupControlPollFailure {
    return when (error) {
        is org.apache.kafka.common.errors.AuthenticationException -> WarmupControlPollFailure.Terminal(
            WarmupControlPollFailureReason.AUTHENTICATION
        )
        is org.apache.kafka.common.errors.AuthorizationException -> WarmupControlPollFailure.Terminal(
            WarmupControlPollFailureReason.AUTHORIZATION
        )
        is KafkaTimeoutException -> WarmupControlPollFailure.Temporary(
            WarmupControlPollFailureReason.TIMEOUT
        )
        is RetriableException -> WarmupControlPollFailure.Temporary(
            WarmupControlPollFailureReason.RETRIABLE
        )
        else -> WarmupControlPollFailure.Terminal(WarmupControlPollFailureReason.OTHER)
    }
}

internal tailrec fun unwrapKafkaMetadataFailure(error: Throwable): Throwable =
    if (error is ExecutionException && error.cause != null) {
        unwrapKafkaMetadataFailure(error.cause!!)
    } else {
        error
    }

private fun immutableProperties(properties: Map<String, Any?>): Map<String, Any?> =
    Collections.unmodifiableMap(LinkedHashMap(properties))
