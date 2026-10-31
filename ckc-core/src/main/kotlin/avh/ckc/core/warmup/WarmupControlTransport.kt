package avh.ckc.core.warmup

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
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
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
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
import kotlin.coroutines.resumeWithException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

internal class WarmupControlTransportConfiguration(
    val instanceId: WarmupInstanceId,
    val topic: String,
    producerProperties: Map<String, Any?>,
    consumerProperties: Map<String, Any?>,
    val groupIdPrefix: String = "ckc-traffic-warmup-control-v1",
    val pollTimeout: Duration = 100.milliseconds,
    val sendTimeout: Duration = 30.seconds,
    val commandBufferCapacity: Int = 1024
) {
    val producerProperties: Map<String, Any?> = immutableProperties(producerProperties)
    val consumerProperties: Map<String, Any?> = immutableProperties(consumerProperties)
    val groupId: String = "$groupIdPrefix-${instanceId.value}"

    init {
        validateTopicName("controlTopic", topic)
        require(groupIdPrefix.isNotBlank()) { "groupIdPrefix must not be blank" }
        require(groupIdPrefix.none(Char::isISOControl)) { "groupIdPrefix must not contain control characters" }
        require(pollTimeout.isPositive()) { "pollTimeout must be positive" }
        require(sendTimeout.isPositive()) { "sendTimeout must be positive" }
        require(commandBufferCapacity > 0) { "commandBufferCapacity must be positive" }
        require(this.producerProperties.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            "Control producer bootstrap servers are required"
        }
        require(this.consumerProperties.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            "Control consumer bootstrap servers are required"
        }
    }
}

internal fun interface WarmupControlMessageHandler {
    suspend fun onMessage(message: WarmupControlMessage)
}

internal fun interface WarmupInvalidControlRecordHandler {
    fun onInvalidRecord(key: String?, error: Throwable)

    companion object {
        val NOOP = WarmupInvalidControlRecordHandler { _, _ -> }
    }
}

/**
 * One control consumer per coordinator scope. Its instance-specific group receives the complete
 * control stream, while the dispatch coroutine keeps session work out of the Kafka poll loop.
 */
internal class WarmupControlTransport(
    private val configuration: WarmupControlTransportConfiguration,
    private val handler: WarmupControlMessageHandler,
    private val invalidRecordHandler: WarmupInvalidControlRecordHandler = WarmupInvalidControlRecordHandler.NOOP,
    parentContext: CoroutineContext = Dispatchers.IO,
    private val producerFactory: (Map<String, Any?>) -> KafkaProducer<String, ByteArray> = { KafkaProducer(it) },
    private val consumerFactory: (Map<String, Any?>) -> KafkaConsumer<String, ByteArray> = { KafkaConsumer(it) },
    private val dispatcher: CoroutineDispatcher = Dispatchers.IO
) {
    private val lifecycleJob = SupervisorJob(parentContext[Job])
    private val scope = CoroutineScope(parentContext + lifecycleJob)
    private val commands = Channel<WarmupControlMessage>(configuration.commandBufferCapacity)
    private val readiness = CompletableDeferred<Unit>()
    private val termination = CompletableDeferred<Throwable?>()
    private val failure = AtomicReference<Throwable?>()
    private val started = AtomicBoolean(false)
    private val stopping = AtomicBoolean(false)
    private var producer: KafkaProducer<String, ByteArray>? = null
    private var consumer: KafkaConsumer<String, ByteArray>? = null
    private var pollJob: Job? = null
    private var dispatchJob: Job? = null

    fun start() {
        check(started.compareAndSet(false, true)) { "Warm-up control transport already started" }
        try {
            producer = producerFactory(producerProperties())
            consumer = consumerFactory(consumerProperties())
            dispatchJob = scope.launch { dispatchLoop() }.also(::observe)
            pollJob = scope.launch { pollLoop(requireNotNull(consumer)) }.also(::observe)
        } catch (error: Throwable) {
            fail(error)
            runCatching { consumer?.close(JavaDuration.ofMillis(configuration.sendTimeout.inWholeMilliseconds)) }
            runCatching { producer?.close(JavaDuration.ofMillis(configuration.sendTimeout.inWholeMilliseconds)) }
            throw error
        }
    }

    suspend fun awaitReady() {
        check(started.get()) { "Warm-up control transport has not started" }
        readiness.await()
        failure.get()?.let { throw it }
    }

    suspend fun send(message: WarmupControlMessage): RecordMetadata {
        check(message.senderInstanceId == configuration.instanceId) {
            "Control message sender must match the local instance"
        }
        awaitReady()
        failure.get()?.let { throw it }
        check(!stopping.get()) { "Warm-up control transport is stopping" }
        val record = ProducerRecord(
            configuration.topic,
            message.sessionId.value,
            WarmupControlCodec.encode(message)
        )
        return withTimeout(configuration.sendTimeout) { sendAcknowledged(requireNotNull(producer), record) }
    }

    suspend fun awaitTermination() {
        termination.await()?.let { throw it }
    }

    suspend fun stop() {
        if (!stopping.compareAndSet(false, true)) {
            pollJob?.join()
            dispatchJob?.join()
            return
        }
        consumer?.wakeup()
        pollJob?.cancelAndJoin()
        commands.close()
        dispatchJob?.cancelAndJoin()
        withContext(dispatcher) {
            runCatching { producer?.close(JavaDuration.ofMillis(configuration.sendTimeout.inWholeMilliseconds)) }
            runCatching { consumer?.close(JavaDuration.ofMillis(configuration.sendTimeout.inWholeMilliseconds)) }
        }
        if (!readiness.isCompleted) {
            readiness.completeExceptionally(CancellationException("Warm-up control transport stopped before readiness"))
        }
        termination.complete(failure.get())
        scope.cancel()
    }

    private suspend fun pollLoop(activeConsumer: KafkaConsumer<String, ByteArray>) {
        val initializedPartitions = mutableSetOf<TopicPartition>()
        try {
            activeConsumer.subscribe(listOf(configuration.topic), object : ConsumerRebalanceListener {
                override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) = Unit

                override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                    val newPartitions = partitions.filterNot(initializedPartitions::contains)
                    if (newPartitions.isNotEmpty()) {
                        activeConsumer.seekToEnd(newPartitions)
                        newPartitions.forEach { activeConsumer.position(it) }
                        initializedPartitions += newPartitions
                    }
                    if (partitions.isNotEmpty()) readiness.complete(Unit)
                }
            })
            while (currentCoroutineContext().isActive && !stopping.get()) {
                val records = try {
                    activeConsumer.poll(JavaDuration.ofMillis(configuration.pollTimeout.inWholeMilliseconds))
                } catch (wakeup: WakeupException) {
                    if (stopping.get()) break else throw wakeup
                }
                for (record in records) {
                    val message = try {
                        WarmupControlCodec.decode(record.value()).also {
                            require(record.key() == it.sessionId.value) {
                                "Control record key does not match its session"
                            }
                        }
                    } catch (error: Throwable) {
                        invalidRecordHandler.onInvalidRecord(record.key(), error)
                        continue
                    }
                    check(commands.trySend(message).isSuccess) {
                        "Warm-up control command buffer is full or closed"
                    }
                }
            }
        } finally {
            withContext(NonCancellable + dispatcher) { runCatching { activeConsumer.close() } }
        }
    }

    private suspend fun dispatchLoop() {
        for (message in commands) handler.onMessage(message)
    }

    private fun observe(job: Job) {
        job.invokeOnCompletion { error ->
            if (error != null && error !is CancellationException) fail(error)
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

    private fun producerProperties(): Map<String, Any?> = configuration.producerProperties + mapOf(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
        ProducerConfig.ACKS_CONFIG to "all",
        ProducerConfig.CLIENT_ID_CONFIG to "${configuration.groupId}-producer"
    )

    private fun consumerProperties(): Map<String, Any?> = configuration.consumerProperties + mapOf(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
        ConsumerConfig.GROUP_ID_CONFIG to configuration.groupId,
        ConsumerConfig.CLIENT_ID_CONFIG to "${configuration.groupId}-consumer",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "latest"
    )

    private suspend fun sendAcknowledged(
        activeProducer: KafkaProducer<String, ByteArray>,
        record: ProducerRecord<String, ByteArray>
    ): RecordMetadata = suspendCancellableCoroutine { continuation ->
        try {
            activeProducer.send(record) { metadata, error ->
                when {
                    error != null -> continuation.resumeWith(Result.failure(error))
                    metadata == null -> continuation.resumeWithException(
                        IllegalStateException("Control producer acknowledgement did not contain metadata")
                    )
                    else -> continuation.resumeWith(Result.success(metadata))
                }
            }
        } catch (error: Throwable) {
            continuation.resumeWithException(error)
        }
    }
}

private fun immutableProperties(properties: Map<String, Any?>): Map<String, Any?> =
    Collections.unmodifiableMap(LinkedHashMap(properties))
