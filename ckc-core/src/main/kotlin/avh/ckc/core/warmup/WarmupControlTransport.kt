package avh.ckc.core.warmup

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
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
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
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
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
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
    val topic: String,
    producerProperties: Map<String, Any?>,
    consumerProperties: Map<String, Any?>,
    val groupIdPrefix: String = "ckc.traffic-warmup.v1.control",
    val pollTimeout: Duration = 100.milliseconds,
    val sendTimeout: Duration = 30.seconds,
    val commandBufferCapacity: Int = 1024
) {
    val producerProperties: Map<String, Any?> = immutableProperties(producerProperties)
    val consumerProperties: Map<String, Any?> = immutableProperties(consumerProperties)
    val groupId: String = "$groupIdPrefix-${instanceId.value}"

    init {
        require(topic.isNotBlank()) { "Control topic must not be blank" }
        require(topic.length <= 249) { "Control topic must be at most 249 characters" }
        require(topic.none(Char::isISOControl)) { "Control topic must not contain control characters" }
        require(groupIdPrefix.isNotBlank()) { "Control group id prefix must not be blank" }
        require(pollTimeout.isPositive() && pollTimeout.inWholeMilliseconds > 0) {
            "Control poll timeout must be at least one millisecond"
        }
        require(sendTimeout.isPositive() && sendTimeout.inWholeMilliseconds > 0) {
            "Control send timeout must be at least one millisecond"
        }
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

internal interface WarmupControlChannel {
    fun start()
    suspend fun awaitReady()
    suspend fun send(message: WarmupControlMessage): RecordMetadata
    /** Null is reserved for in-memory test channels that do not model Kafka metadata. */
    suspend fun topicPartitions(topics: Set<String>): Map<String, Set<Int>>? = null
    fun initialOffsetBoundary(): Long? = null
    suspend fun awaitTermination()
    suspend fun stop()
}

/** One Kafka consumer per library context; dispatch is kept off the business pipeline. */
internal class WarmupControlTransport(
    private val configuration: WarmupControlTransportConfiguration,
    private val handler: WarmupControlMessageHandler,
    private val invalidRecordHandler: WarmupInvalidControlRecordHandler = WarmupInvalidControlRecordHandler.FAIL,
    parentContext: CoroutineContext = Dispatchers.IO,
    private val producerFactory: (Map<String, Any?>) -> Producer<String, ByteArray> = { KafkaProducer(it) },
    private val consumerFactory: (Map<String, Any?>) -> Consumer<String, ByteArray> = { KafkaConsumer(it) },
    private val dispatcher: CoroutineDispatcher = Dispatchers.IO
) : WarmupControlChannel {
    private sealed interface DispatchItem {
        data class Record(val record: WarmupControlRecord) : DispatchItem
        data class InvalidRecord(val key: String?, val error: Throwable) : DispatchItem
    }

    private val lifecycleJob = SupervisorJob(parentContext[Job])
    private val scope = CoroutineScope(parentContext + lifecycleJob)
    private val shutdownScope = CoroutineScope(dispatcher)
    private val commands = Channel<DispatchItem>(configuration.commandBufferCapacity)
    private val readiness = CompletableDeferred<Unit>()
    private val termination = CompletableDeferred<Throwable?>()
    private val failure = AtomicReference<Throwable?>()
    private val started = AtomicBoolean(false)
    private val stopping = AtomicBoolean(false)
    private val shutdownJob = AtomicReference<Job?>()
    private var producer: Producer<String, ByteArray>? = null
    private var consumer: Consumer<String, ByteArray>? = null
    private var pollJob: Job? = null
    private var dispatchJob: Job? = null
    private var initializedPartition: TopicPartition? = null
    @Volatile private var initialBoundary: Long? = null

    override fun initialOffsetBoundary(): Long? = initialBoundary

    override fun start() {
        check(started.compareAndSet(false, true)) { "Warm-up control transport already started" }
        try {
            producer = producerFactory(producerProperties())
            consumer = consumerFactory(consumerProperties())
            dispatchJob = scope.launch { dispatchLoop() }.also(::observe)
            pollJob = scope.launch { pollLoop(requireNotNull(consumer)) }.also(::observe)
        } catch (error: Throwable) {
            fail(error)
            closeClients()
            throw error
        }
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
        awaitReady()
        failure.get()?.let { throw it }
        check(!stopping.get()) { "Warm-up control transport is stopping" }
        val record = ProducerRecord(configuration.topic, message.sessionId.value, WarmupControlCodec.encode(message))
        return withTimeout(configuration.sendTimeout) {
            sendAcknowledged(requireNotNull(producer), record)
        }
    }

    override suspend fun topicPartitions(topics: Set<String>): Map<String, Set<Int>> {
        awaitReady()
        require(topics.isNotEmpty()) { "At least one metadata topic is required" }
        return runInterruptible(dispatcher) {
            topics.associateWith { topic ->
                requireNotNull(producer).partitionsFor(topic).mapTo(linkedSetOf()) { it.partition() }
                    .also { require(it.isNotEmpty()) { "Kafka metadata returned no partitions for $topic" } }
            }
        }
    }

    override suspend fun awaitTermination() {
        termination.await()?.let { throw it }
    }

    override suspend fun stop() {
        shutdownOperation().join()
    }

    private suspend fun pollLoop(activeConsumer: Consumer<String, ByteArray>) {
        try {
            activeConsumer.subscribe(listOf(configuration.topic), object : ConsumerRebalanceListener {
                override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) = Unit

                override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                    check(partitions.size == 1) { "Warm-up control topic must have exactly one partition" }
                    if (initializedPartition == null) {
                        val partition = partitions.single()
                        val end = activeConsumer.endOffsets(partitions).getValue(partition)
                        activeConsumer.seek(partition, end)
                        activeConsumer.position(partition)
                        initialBoundary = end - 1
                        initializedPartition = partition
                        readiness.complete(Unit)
                    }
                }
            })
            while (currentCoroutineContext().isActive && !stopping.get()) {
                val records = try {
                    activeConsumer.poll(JavaDuration.ofMillis(configuration.pollTimeout.inWholeMilliseconds))
                } catch (wakeup: WakeupException) {
                    if (stopping.get()) break else throw wakeup
                }
                records.forEach { record ->
                    val message = try {
                        WarmupControlCodec.decode(record.value()).also {
                            require(record.key() == it.sessionId.value) {
                                "Control record key does not match its session"
                            }
                        }
                    } catch (error: Exception) {
                        enqueue(DispatchItem.InvalidRecord(record.key(), error))
                        return@forEach
                    }
                    enqueue(DispatchItem.Record(WarmupControlRecord(record.offset(), message)))
                }
            }
        } finally {
            withContext(NonCancellable + dispatcher) { runCatching { activeConsumer.close(closeDuration()) } }
        }
    }

    private suspend fun dispatchLoop() {
        for (item in commands) {
            when (item) {
                is DispatchItem.Record -> handler.onMessage(item.record)
                is DispatchItem.InvalidRecord -> invalidRecordHandler.onInvalidRecord(item.key, item.error)
            }
        }
    }

    private fun enqueue(item: DispatchItem) {
        check(commands.trySend(item).isSuccess) { "Warm-up control command buffer is full or closed" }
    }

    private suspend fun sendAcknowledged(
        activeProducer: Producer<String, ByteArray>,
        record: ProducerRecord<String, ByteArray>
    ): RecordMetadata {
        val acknowledgement = CompletableDeferred<RecordMetadata>()
        runInterruptible(dispatcher) {
            activeProducer.send(record) { metadata, error ->
                when {
                    error != null -> acknowledgement.completeExceptionally(error)
                    metadata == null -> acknowledgement.completeExceptionally(
                        IllegalStateException("Control producer acknowledgement did not contain metadata")
                    )
                    else -> acknowledgement.complete(metadata)
                }
            }
        }
        return acknowledgement.await()
    }

    private fun observe(job: Job) {
        job.invokeOnCompletion { error ->
            if (error != null && !isExpectedCancellation(error)) fail(error)
        }
    }

    private fun isExpectedCancellation(error: Throwable): Boolean =
        error is CancellationException && (stopping.get() || failure.get() != null || !lifecycleJob.isActive)

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
        withContext(dispatcher) { closeClients() }
        if (!readiness.isCompleted) {
            readiness.completeExceptionally(CancellationException("Warm-up control transport stopped before readiness"))
        }
        termination.complete(failure.get())
        scope.cancel()
    }

    private fun closeClients() {
        runCatching { producer?.close(closeDuration()) }
        runCatching { consumer?.close(closeDuration()) }
    }

    private fun closeDuration() = JavaDuration.ofMillis(configuration.sendTimeout.inWholeMilliseconds)

    private fun producerProperties(): Map<String, Any?> = configuration.producerProperties + mapOf(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
        ProducerConfig.ACKS_CONFIG to "all",
        ProducerConfig.CLIENT_ID_CONFIG to "${configuration.groupId}-producer",
        ProducerConfig.MAX_BLOCK_MS_CONFIG to configuration.sendTimeout.inWholeMilliseconds
    )

    private fun consumerProperties(): Map<String, Any?> = configuration.consumerProperties + mapOf(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
        ConsumerConfig.GROUP_ID_CONFIG to configuration.groupId,
        ConsumerConfig.CLIENT_ID_CONFIG to "${configuration.groupId}-consumer",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
    )
}

private fun immutableProperties(properties: Map<String, Any?>): Map<String, Any?> =
    Collections.unmodifiableMap(LinkedHashMap(properties))
