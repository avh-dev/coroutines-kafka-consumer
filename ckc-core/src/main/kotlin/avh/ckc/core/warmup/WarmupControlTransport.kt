package avh.ckc.core.warmup

import avh.ckc.core.isFatalJvmError

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
import org.apache.kafka.clients.producer.ProducerRecord
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
import java.time.Duration as JavaDuration
import java.util.Collections
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

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
    val commandBufferCapacity: Int = 1_024
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

internal fun interface WarmupInvalidControlRecordHandler {
    fun onInvalidRecord(key: String?, error: Throwable)

    companion object {
        val FAIL = WarmupInvalidControlRecordHandler { key, error ->
            throw InvalidWarmupControlRecordException(key, error)
        }
    }
}

internal class InvalidWarmupControlRecordException(key: String?, cause: Throwable) :
    IllegalArgumentException("Invalid warm-up control record with key ${key ?: "<null>"}", cause)

internal class WarmupControlSendTimeoutException(timeout: Duration) :
    IllegalStateException("Warm-up control send exceeded $timeout")

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
    @Volatile private var expectedTopicIdentity: String? = null

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
        val record = ProducerRecord(configuration.topic, message.sessionId.value, WarmupControlCodec.encode(message))
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
        val cursor = WarmupControlCursor()
        while (currentCoroutineContext().isActive && !stopping.get()) {
            try {
                pollControlTopic(activeConsumer, cursor)
                return
            } catch (recreated: WarmupControlTopicRecreatedException) {
                if (stopping.get()) return
                expectedTopicIdentity = recreated.currentIdentity
                cursor.nextOffset = null
                consumer = null
                replaceProducerAfterTopicRecreation()
                delay(configuration.recoveryRetryBackoff)
                activeConsumer = consumerFactory(consumerProperties()).also { consumer = it }
            } catch (recovery: WarmupControlRecoveryRequiredException) {
                if (stopping.get()) return
                log.warn("Warm-up control consumer requires a bounded replacement", recovery)
                consumer = null
                delay(configuration.recoveryRetryBackoff)
                activeConsumer = consumerFactory(consumerProperties()).also { consumer = it }
            }
        }
    }

    private suspend fun pollControlTopic(
        activeConsumer: Consumer<String, ByteArray>,
        cursor: WarmupControlCursor
    ) {
        var assignmentState: WarmupControlAssignmentState? = null
        try {
            val metadata = activeConsumer.partitionsFor(
                configuration.topic,
                JavaDuration.ofMillis(configuration.metadataTimeout.inWholeMilliseconds)
            )
            check(metadata.size == 1 && metadata.single().partition() == 0) {
                "Warm-up control topic must expose exactly partition 0"
            }
            assignmentState = awaitingAssignment()
            activeConsumer.subscribe(listOf(configuration.topic), object : ConsumerRebalanceListener {
                override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
                    (assignmentState as? WarmupControlAssignmentState.Active)?.identityCheck?.cancel()
                    assignmentState = awaitingAssignment()
                }

                override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                    try {
                        initializeAssignedPartition(partitions)
                    } catch (error: Throwable) {
                        if (error.isFatalJvmError()) throw error
                        assignmentState = WarmupControlAssignmentState.CallbackFailed(error)
                    }
                }

                private fun initializeAssignedPartition(partitions: Collection<TopicPartition>) {
                    check(partitions.size == 1) { "Warm-up control topic must have exactly one partition" }
                    val partition = partitions.single()
                    check(partition.topic() == configuration.topic && partition.partition() == 0) {
                        "Warm-up control topic must use partition 0"
                    }
                    val awaiting = assignmentState as? WarmupControlAssignmentState.Awaiting
                        ?: awaitingAssignment()
                    val currentTopicIdentity = withinAssignmentDeadline(
                        awaiting,
                        "checking the assigned control topic identity"
                    ) { timeout ->
                        topicIdentity.read(configuration.topic, timeout.toMillis().milliseconds)
                    }
                    if (!readiness.isCompleted) {
                        expectedTopicIdentity = currentTopicIdentity
                        val end = withinAssignmentDeadline(awaiting, "reading the initial log end") { timeout ->
                            activeConsumer.endOffsets(partitions.toSet(), timeout).getValue(partition)
                        }
                        activeConsumer.seek(partition, end)
                        withinAssignmentDeadline(awaiting, "initializing the initial position") { timeout ->
                            activeConsumer.position(partition, timeout)
                        }
                        cursor.nextOffset = end
                        initialBoundary = end - 1
                        readiness.complete(Unit)
                    } else {
                        val expectedIdentity = requireNotNull(expectedTopicIdentity)
                        if (currentTopicIdentity != expectedIdentity) {
                            assignmentState = WarmupControlAssignmentState.TopicRecreated(
                                expectedIdentity,
                                currentTopicIdentity
                            )
                            return
                        }
                        cursor.nextOffset?.let { activeConsumer.seek(partition, it) }
                        withinAssignmentDeadline(awaiting, "initializing the replacement position") { timeout ->
                            activeConsumer.position(partition, timeout).also { cursor.nextOffset = it }
                        }
                    }
                    assignmentState = activeAssignment()
                }
            })
            while (currentCoroutineContext().isActive && !stopping.get()) {
                val records = try {
                    activeConsumer.poll(JavaDuration.ofMillis(configuration.pollTimeout.inWholeMilliseconds))
                } catch (wakeup: WakeupException) {
                    if (stopping.get()) break else throw wakeup
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
                        is WarmupControlPollFailure.Terminal -> {
                            throw error
                        }
                    }
                }
                (assignmentState as? WarmupControlAssignmentState.TopicRecreated)?.let { recreated ->
                    throw WarmupControlTopicRecreatedException(
                        recreated.previousIdentity,
                        recreated.currentIdentity
                    )
                }
                (assignmentState as? WarmupControlAssignmentState.CallbackFailed)?.let { failed ->
                    throw failed.error
                }
                records.forEach { record ->
                    cursor.nextOffset = maxOf(cursor.nextOffset ?: 0L, record.offset() + 1)
                    val message = try {
                        WarmupControlCodec.decode(record.value()).also { decoded ->
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
                val now = nanoTime()
                when (val state = requireNotNull(assignmentState)) {
                    is WarmupControlAssignmentState.Awaiting -> {
                        if (now >= state.deadlineNanos) {
                            throw WarmupControlAssignmentTimeoutException(configuration.assignmentTimeout)
                        }
                    }
                    is WarmupControlAssignmentState.Active -> {
                        state.identityCheck?.takeIf { it.isCompleted }?.let {
                            state.identityCheck = null
                            when (val check = it.await()) {
                                WarmupControlTopicIdentityCheck.Unavailable -> Unit
                                is WarmupControlTopicIdentityCheck.Terminal -> throw check.error
                                is WarmupControlTopicIdentityCheck.Available -> {
                                    val expected = requireNotNull(expectedTopicIdentity)
                                    if (check.identity != expected) {
                                        throw WarmupControlTopicRecreatedException(expected, check.identity)
                                    }
                                }
                            }
                            state.nextIdentityCheckNanos = now +
                                configuration.topicIdentityCheckInterval.inWholeNanoseconds
                        }
                        if (state.identityCheck == null && now >= state.nextIdentityCheckNanos) {
                            state.identityCheck = scope.async { checkTopicIdentity() }
                        }
                    }
                    is WarmupControlAssignmentState.TopicRecreated -> error("Topic recreation must be handled first")
                    is WarmupControlAssignmentState.CallbackFailed -> error("Callback failure must be handled first")
                }
            }
        } finally {
            (assignmentState as? WarmupControlAssignmentState.Active)?.identityCheck?.cancel()
            withContext(NonCancellable + dispatcher) {
                runCatching { activeConsumer.close(closeDuration()) }
            }
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
        val cause = unwrapControlMetadataFailure(error)
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
            val cause = unwrapControlMetadataFailure(error)
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

    private fun producerProperties(): Map<String, Any?> = buildControlProducerProperties(
        configuration.producerProperties,
        mapOf(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.CLIENT_ID_CONFIG to "${configuration.groupId}-producer",
            ProducerConfig.MAX_BLOCK_MS_CONFIG to configuration.sendTimeout.inWholeMilliseconds
        )
    )

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

private open class WarmupControlRecoveryRequiredException(
    message: String
) : IllegalStateException(message)

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

private sealed interface WarmupControlTopicIdentityCheck {
    data class Available(val identity: String) : WarmupControlTopicIdentityCheck
    data class Terminal(val error: Throwable) : WarmupControlTopicIdentityCheck
    data object Unavailable : WarmupControlTopicIdentityCheck
}

private enum class WarmupControlPollFailureReason {
    TIMEOUT,
    RETRIABLE,
    AUTHENTICATION,
    AUTHORIZATION,
    OTHER
}

internal interface WarmupControlTopicIdentity : AutoCloseable {
    fun read(topic: String, timeout: Duration): String
    override fun close() = Unit
}

private class WarmupControlCursor(var nextOffset: Long? = null)

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

private class WarmupControlTopicRecreatedException(
    previousIdentity: String,
    val currentIdentity: String
) : WarmupControlRecoveryRequiredException(
    "Warm-up control topic identity changed from $previousIdentity to $currentIdentity"
)

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

private tailrec fun unwrapControlMetadataFailure(error: Throwable): Throwable =
    if (error is ExecutionException && error.cause != null) {
        unwrapControlMetadataFailure(error.cause!!)
    } else {
        error
    }

private fun immutableProperties(properties: Map<String, Any?>): Map<String, Any?> =
    Collections.unmodifiableMap(LinkedHashMap(properties))
