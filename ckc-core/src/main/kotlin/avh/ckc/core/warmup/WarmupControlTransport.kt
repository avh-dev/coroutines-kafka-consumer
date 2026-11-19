package avh.ckc.core.warmup

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
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
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException as KafkaTimeoutException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.time.Duration as JavaDuration
import java.util.Collections
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
    val recoveryCheckInterval: Duration = 1.seconds,
    val recoveryRetryBackoff: Duration = 500.milliseconds,
    val recoveryFailureThreshold: Int = 3,
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
        require(recoveryCheckInterval.isPositive()) { "Control recovery check interval must be positive" }
        require(recoveryRetryBackoff.isPositive()) { "Control recovery retry backoff must be positive" }
        require(recoveryFailureThreshold > 0) { "Control recovery failure threshold must be positive" }
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
            } catch (_: WarmupControlTopicRecreatedException) {
                if (stopping.get()) return
                cursor.nextOffset = null
                consumer = null
                replaceProducerAfterTopicRecreation()
                delay(configuration.recoveryRetryBackoff)
                activeConsumer = consumerFactory(consumerProperties()).also { consumer = it }
            } catch (reset: WarmupControlLogTruncatedException) {
                if (stopping.get()) return
                cursor.nextOffset = reset.endOffset
                consumer = null
                delay(configuration.recoveryRetryBackoff)
                activeConsumer = consumerFactory(consumerProperties()).also { consumer = it }
            } catch (recovery: WarmupControlRecoveryRequiredException) {
                if (stopping.get()) return
                log.warn("Warm-up control consumer requires a bounded replacement", recovery)
                consumer = null
                delay(configuration.recoveryRetryBackoff)
                activeConsumer = consumerFactory(consumerProperties()).also { consumer = it }
            } catch (timeout: KafkaTimeoutException) {
                if (!readiness.isCompleted || stopping.get()) throw timeout
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
        try {
            val metadata = activeConsumer.partitionsFor(
                configuration.topic,
                JavaDuration.ofMillis(configuration.metadataTimeout.inWholeMilliseconds)
            )
            check(metadata.size == 1 && metadata.single().partition() == 0) {
                "Warm-up control topic must expose exactly partition 0"
            }
            if (expectedTopicIdentity == null) {
                expectedTopicIdentity = readTopicIdentity(configuration.metadataTimeout)
                    ?: error("Warm-up control topic identity is unavailable")
            }
            var assignmentState: WarmupControlAssignmentState = awaitingAssignment()
            activeConsumer.subscribe(listOf(configuration.topic), object : ConsumerRebalanceListener {
                override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
                    assignmentState = awaitingAssignment()
                }

                override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                    check(partitions.size == 1) { "Warm-up control topic must have exactly one partition" }
                    val partition = partitions.single()
                    check(partition.topic() == configuration.topic && partition.partition() == 0) {
                        "Warm-up control topic must use partition 0"
                    }
                    val awaiting = assignmentState as? WarmupControlAssignmentState.Awaiting
                        ?: awaitingAssignment()
                    if (!readiness.isCompleted) {
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
                        cursor.nextOffset?.let { nextOffset ->
                            activeConsumer.seek(partition, nextOffset)
                        }
                        // Position initialization also applies to a confirmed fresh topic, where
                        // the cursor is intentionally empty and Kafka's earliest reset is retained.
                        withinAssignmentDeadline(awaiting, "initializing the replacement position") { timeout ->
                            activeConsumer.position(partition, timeout)
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
                when (val state = assignmentState) {
                    is WarmupControlAssignmentState.Awaiting -> {
                        if (now >= state.deadlineNanos) {
                            throw WarmupControlAssignmentTimeoutException(configuration.assignmentTimeout)
                        }
                    }
                    is WarmupControlAssignmentState.Active -> {
                        if (now >= state.nextRecoveryCheckNanos) {
                            if (verifyCurrentControlLog(activeConsumer, cursor)) {
                                state.consecutiveRecoveryFailures = 0
                            } else if (++state.consecutiveRecoveryFailures >=
                                configuration.recoveryFailureThreshold
                            ) {
                                throw WarmupControlRecoveryRequiredException(
                                    "Warm-up control consumer could not validate its position after " +
                                        "${state.consecutiveRecoveryFailures} attempts"
                                )
                            }
                            state.nextRecoveryCheckNanos = now +
                                configuration.recoveryCheckInterval.inWholeNanoseconds
                        }
                    }
                }
            }
        } finally {
            withContext(NonCancellable + dispatcher) {
                runCatching { activeConsumer.close(closeDuration()) }
            }
        }
    }

    private fun awaitingAssignment() = WarmupControlAssignmentState.Awaiting(
        nanoTime() + configuration.assignmentTimeout.inWholeNanoseconds
    )

    private fun activeAssignment() = WarmupControlAssignmentState.Active(
        nanoTime() + configuration.recoveryCheckInterval.inWholeNanoseconds
    )

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
        } catch (timeout: KafkaTimeoutException) {
            throw WarmupControlAssignmentTimeoutException(
                configuration.assignmentTimeout,
                operation,
                timeout
            )
        }
    }

    private suspend fun verifyCurrentControlLog(
        activeConsumer: Consumer<String, ByteArray>,
        cursor: WarmupControlCursor
    ): Boolean {
        val currentIdentity = readTopicIdentity(
            minOf(configuration.metadataTimeout, configuration.recoveryCheckInterval)
        ) ?: return false
        verifyTopicIdentity(currentIdentity)
        return verifyConsumerPosition(activeConsumer, cursor)
    }

    /** Topic identity, rather than a transient metadata failure, defines a fresh control log. */
    private fun verifyTopicIdentity(currentIdentity: String) {
        val expectedIdentity = expectedTopicIdentity
        if (expectedIdentity != null && currentIdentity != expectedIdentity) {
            expectedTopicIdentity = currentIdentity
            throw WarmupControlTopicRecreatedException(expectedIdentity, currentIdentity)
        }
    }

    /** A replacement client for the same log must resume after every record already admitted locally. */
    private fun verifyConsumerPosition(
        activeConsumer: Consumer<String, ByteArray>,
        cursor: WarmupControlCursor
    ): Boolean {
        val assignment = activeConsumer.assignment()
        if (assignment.size != 1) return false
        val partition = assignment.single()
        return try {
            val timeout = JavaDuration.ofMillis(
                minOf(
                    configuration.metadataTimeout.inWholeMilliseconds,
                    configuration.recoveryCheckInterval.inWholeMilliseconds
                ).coerceAtLeast(1)
            )
            val position = activeConsumer.position(partition, timeout)
            val end = activeConsumer.endOffsets(setOf(partition), timeout).getValue(partition)
            if (position > end) throw WarmupControlLogTruncatedException(position, end)
            cursor.nextOffset = maxOf(cursor.nextOffset ?: 0L, position)
            true
        } catch (truncated: WarmupControlLogTruncatedException) {
            throw truncated
        } catch (_: Exception) {
            // Kafka reconnect remains authoritative for ordinary transient metadata failures.
            false
        }
    }

    private suspend fun readTopicIdentity(timeout: Duration): String? = runInterruptible(dispatcher) {
        topicIdentity.read(configuration.topic, timeout)
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

private open class WarmupControlRecoveryRequiredException(message: String) : IllegalStateException(message)

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

internal interface WarmupControlTopicIdentity : AutoCloseable {
    fun read(topic: String, timeout: Duration): String?
    override fun close() = Unit
}

private class WarmupControlCursor(var nextOffset: Long? = null)

/** Recovery checks are valid only after the current assignment has been positioned. */
private sealed interface WarmupControlAssignmentState {
    data class Awaiting(val deadlineNanos: Long) : WarmupControlAssignmentState

    data class Active(
        var nextRecoveryCheckNanos: Long,
        var consecutiveRecoveryFailures: Int = 0
    ) : WarmupControlAssignmentState
}

internal class KafkaWarmupControlTopicIdentity(
    properties: Map<String, Any?>,
    private val closeTimeout: Duration
) : WarmupControlTopicIdentity {
    private val admin = Admin.create(properties)

    override fun read(topic: String, timeout: Duration): String? = try {
        val timeoutMillis = timeout.inWholeMilliseconds.coerceIn(1, Int.MAX_VALUE.toLong())
        admin.describeTopics(listOf(topic), DescribeTopicsOptions().timeoutMs(timeoutMillis.toInt()))
            .allTopicNames()
            .get(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
            .getValue(topic)
            .topicId()
            .toString()
    } catch (_: Exception) {
        null
    }

    override fun close() = admin.close(
        JavaDuration.ofMillis(closeTimeout.inWholeMilliseconds.coerceAtLeast(1))
    )
}

private class WarmupControlTopicRecreatedException(previous: String, current: String) :
    WarmupControlRecoveryRequiredException(
        "Warm-up control topic was recreated with identity $current instead of $previous"
    )

private class WarmupControlLogTruncatedException(position: Long, val endOffset: Long) :
    WarmupControlRecoveryRequiredException(
        "Warm-up control position $position exceeds current log end $endOffset"
    )

private fun immutableProperties(properties: Map<String, Any?>): Map<String, Any?> =
    Collections.unmodifiableMap(LinkedHashMap(properties))
