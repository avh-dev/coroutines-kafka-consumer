package avh.ckc.warmup

import avh.ckc.core.ConsumerContainer
import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.ProcessingMode
import avh.ckc.core.consumerContainer
import avh.ckc.core.coroutinesKafkaConsumer
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.warmup.broker.BrokerLifecycleEvent
import avh.ckc.warmup.broker.KafkaBrokerLifecycleLog
import avh.ckc.warmup.broker.launchBrokerLifecycleCli
import avh.ckc.warmup.dataplane.WarmupCompletionOutbox
import avh.ckc.warmup.dataplane.WarmupResultLane
import avh.ckc.warmup.donor.WarmupDonorQuotaPhases
import avh.ckc.warmup.donor.WarmupDonorSessionLifecycle
import avh.ckc.warmup.kafka.WarmupControlChannel
import avh.ckc.warmup.kafka.WarmupControlTransport
import avh.ckc.warmup.protocol.SessionAccepted
import avh.ckc.warmup.protocol.SessionEnded
import avh.ckc.warmup.protocol.WarmupControlCodec
import avh.ckc.warmup.protocol.WarmupControlMessage
import avh.ckc.warmup.protocol.WarmupControlNamespace
import avh.ckc.warmup.protocol.WarmupInstanceId
import avh.ckc.warmup.protocol.WarmupMappingQuota
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupRecordCompleted
import avh.ckc.warmup.protocol.WarmupRecordId
import avh.ckc.warmup.protocol.WarmupRemoteOutcome
import avh.ckc.warmup.protocol.WarmupRequested
import avh.ckc.warmup.protocol.WarmupSessionId
import avh.ckc.warmup.protocol.WarmupTopicMapping
import avh.ckc.warmup.protocol.normalizedAdmissionBudget
import avh.ckc.warmup.recipient.PreparedWarmupRecipientConsumer
import avh.ckc.warmup.recipient.WarmupDestinationStart
import avh.ckc.warmup.runtime.RegisteredTrafficWarmupParticipant
import avh.ckc.warmup.runtime.mappingFingerprint
import java.nio.file.Files
import java.time.Duration as JavaDuration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

/** Kafka lifecycle, builders, and observations shared by context integration tests. */
internal abstract class WarmupContextKafkaFixture {
    protected val kafka: KafkaContainer get() = sharedKafka

    protected suspend fun sessionEndRecords(controlTopic: String, sessionId: WarmupSessionId): List<SessionEnded> =
        sessionControlRecords(controlTopic, sessionId).filterIsInstance<SessionEnded>()

    protected suspend fun sessionControlRecords(
        controlTopic: String,
        sessionId: WarmupSessionId
    ): List<WarmupControlMessage> {
        val properties = consumerProperties(unique("control-audit"), ByteArrayDeserializer::class.java)
        return KafkaConsumer<String, ByteArray>(properties).use { consumer ->
            val partition = TopicPartition(controlTopic, 0)
            consumer.assign(listOf(partition))
            consumer.seekToBeginning(listOf(partition))
            val end = consumer.endOffsets(listOf(partition), JavaDuration.ofSeconds(5)).getValue(partition)
            withTimeout(5.seconds) {
                val messages = mutableListOf<WarmupControlMessage>()
                while (consumer.position(partition, JavaDuration.ofSeconds(1)) < end) {
                    consumer.poll(JavaDuration.ofMillis(100)).forEach { record ->
                        if (record.offset() < end && record.key() == sessionId.value) {
                            messages += WarmupControlCodec.decode(record.value(), record.headers())
                        }
                    }
                }
                messages
            }
        }
    }

    protected fun faultInjectionContext(
        namespace: String,
        controlTopic: String,
        configure: TrafficWarmupContextConfiguration.() -> Unit = {},
        subscriptionAdminFactory: (Map<String, Any?>) -> Admin = Admin::create,
        send: suspend WarmupControlChannel.(WarmupControlMessage) -> RecordMetadata = { send(it) }
    ): TrafficWarmupContext {
        val configuration = TrafficWarmupContextConfiguration().apply {
            this.namespace = namespace
            this.controlTopic = controlTopic
            controlProducerProperties = kafkaConnection()
            controlConsumerProperties = kafkaConnection()
            offerWindow = 500.milliseconds
            preparationTimeout = 3.seconds
            configure()
        }
        return TrafficWarmupContext(configuration, subscriptionAdminFactory) { config, handler, scope, dispatcher ->
            val transport = WarmupControlTransport(
                config, handler, parentContext = scope.coroutineContext, dispatcher = dispatcher
            )
            object : WarmupControlChannel by transport {
                override suspend fun send(message: WarmupControlMessage) = send(transport, message)
            }
        }
    }

    protected suspend fun awaitLifecycle(
        observer: KafkaBrokerLifecycleLog,
        matches: (BrokerLifecycleEvent) -> Boolean
    ): BrokerLifecycleEvent = withTimeout(12.seconds) {
        while (true) {
            val records = observer.poll(JavaDuration.ofMillis(100))
            records.firstOrNull { matches(it.event) }?.let { return@withTimeout it.event }
            yield()
        }
        error("Unreachable")
    }

    protected suspend fun startBrokerWitness(bootstrap: String, topic: String, epoch: String): AutoCloseable {
        val directory = Files.createTempDirectory("ckc-broker-witness")
        val ready = directory.resolve("ready")
        val process = launchBrokerLifecycleCli("serve", bootstrap, topic, epoch, "10000", ready.toString())
        fun close() {
            process.destroyForcibly().waitFor(5, TimeUnit.SECONDS)
            Files.deleteIfExists(ready)
            Files.deleteIfExists(directory)
        }
        try {
            awaitFor {
                check(process.isAlive) { process.inputStream.bufferedReader().readText() }
                Files.exists(ready)
            }
        } catch (error: Throwable) {
            close()
            throw error
        }
        return AutoCloseable { close() }
    }

    protected class ControllableRecipient(
        source: String,
        private val warmup: String,
        override val warmupConsumerProperties: Map<String, Any?>,
        private val stopConsumer: suspend () -> Unit = {},
        private val abortConsumer: suspend () -> Boolean = { false }
    ) : RegisteredTrafficWarmupParticipant {
        override val participantId = WarmupParticipantId("records")
        private val quota = WarmupMappingQuota(10)
        override val mappings = listOf(
            WarmupTopicMapping(
                participantId, source, warmup,
                mappingFingerprint(
                    participantId,
                    TrafficWarmupConsumerConfiguration.Mapping(source, warmup, quota),
                    warmupConsumerProperties
                ),
                quota
            )
        )
        override val mainTopics = listOf(mappings.single().sourceTopic)
        override val mainProperties: Map<String, Any?> = mapOf("bootstrap.servers" to sharedKafka.bootstrapServers)
        override val forwardingProducerProperties: Map<String, Any?> = warmupConsumerProperties
        override val mainConsumer = mock<CoroutinesKafkaConsumer<String, String>>()
        val terminated = CompletableDeferred<Unit>()
        val warmupStopped = CompletableDeferred<Unit>()
        private val resultPublisher = CompletableDeferred<Pair<WarmupSessionId, WarmupResultLane>>()

        init {
            val runtimeControl = mock<avh.ckc.core.extension.ConsumerRuntimeControl>()
            whenever(runtimeControl.isShutdownComplete()).thenReturn(true)
            whenever(mainConsumer.runtimeControl).thenReturn(runtimeControl)
        }

        override fun prepareDonorLane(
            acceptance: SessionAccepted,
            quotaPhases: WarmupDonorQuotaPhases.Session
        ): WarmupDonorSessionLifecycle =
            error("The recipient is never selected as a donor in this test")

        override suspend fun prepareRecipient(
            sessionId: WarmupSessionId,
            start: WarmupDestinationStart,
            completionOutbox: WarmupCompletionOutbox
        ): PreparedWarmupRecipientConsumer {
            resultPublisher.complete(sessionId to completionOutbox.lane("records"))
            return object : PreparedWarmupRecipientConsumer {
                override val assignment = start.assignment
                override val startOffset = start.offset
                override fun start() = Unit
                override suspend fun awaitReady() = Unit
                override suspend fun awaitTermination() = terminated.await()
                override suspend fun stop() {
                    stopConsumer()
                    warmupStopped.complete(Unit)
                }
                override suspend fun abort(): Boolean = abortConsumer().also { stopped ->
                    if (stopped) warmupStopped.complete(Unit)
                }
            }
        }

        suspend fun publishResult() {
            val (session, lane) = resultPublisher.await()
            check(lane.tryAcquire(ConsumerRecord(warmup, 0, 0, "key", "value")))
            lane.publish(
                WarmupRecordCompleted(WarmupRecordId(session, TopicPartition(warmup, 0), 0),
                    WarmupRemoteOutcome.Succeeded)
            )
        }

        override fun startMainRuntime() = Unit
        override suspend fun awaitMainReady() = Unit
        override suspend fun stopMain() = Unit
        override suspend fun awaitMainTermination(): Unit = awaitCancellation()
    }

    protected suspend fun stopApplications(vararg applications: TypedApplication) {
        val failures = applications.mapNotNull { application ->
            runCatching { application.stopAll() }.exceptionOrNull()
        }
        if (failures.isNotEmpty()) {
            throw IllegalStateException("Application cleanup had ${failures.size} failure(s)").apply {
                failures.forEach(::addSuppressed)
            }
        }
    }

    protected fun typedApplication(
        topics: TypedTopics,
        namespace: String,
        warmupDuration: kotlin.time.Duration = 750.milliseconds,
        applicationContext: TrafficWarmupContext? = null,
        lifecycleObserver: (TrafficWarmupLifecycleEvent) -> Unit = {}
    ): TypedApplication {
        val context = applicationContext ?: context(
            namespace,
            topics.control,
            warmupDuration = warmupDuration,
            lifecycleObserver = lifecycleObserver
        )
        val orderMetrics = RecordingWarmupMetrics<String, String>()
        val batchMetrics = RecordingWarmupMetrics<String, ByteArray>()
        val telemetryMetrics = RecordingWarmupMetrics<String, Long>()
        val ordersHandled = CopyOnWriteArrayList<String>()
        val batchesHandled = CopyOnWriteArrayList<ByteArray>()
        val telemetryHandled = CopyOnWriteArrayList<Long>()
        val order = coroutinesKafkaConsumer<String, String>(
            consumerProperties("$namespace-orders", StringDeserializer::class.java)
        ) {
            topics(topics.orders)
            metrics = orderMetrics
            commitIntervalMs = 50
            commitRecordsThreshold = 1
            handle { ordersHandled += it.value() }
            trafficWarmup(context, "orders") {
                mapTopic(topics.orders, topics.warmupOrders) {
                    quotaProfile = "1 -> (0s, step) -> 300"
                }
                warmupConsumerProperties = warmupConsumerConnection()
                forwardingProducerProperties = forwardingConnection(StringSerializer::class.java)
            }
        }
        val batch = coroutinesKafkaConsumer<String, ByteArray>(
            consumerProperties("$namespace-batches", ByteArrayDeserializer::class.java)
        ) {
            topics(topics.batches)
            metrics = batchMetrics
            commitIntervalMs = 50
            commitRecordsThreshold = 1
            handle { batchesHandled += it.value() }
            trafficWarmup(context, "batches") {
                mapTopic(topics.batches, topics.warmupBatches) {
                    quotaProfile = "1 -> (0s, step) -> 300"
                }
                warmupConsumerProperties = warmupConsumerConnection()
                forwardingProducerProperties = forwardingConnection(ByteArraySerializer::class.java)
            }
        }
        val telemetry = coroutinesKafkaConsumer<String, Long>(
            consumerProperties("$namespace-telemetry", LongDeserializer::class.java)
        ) {
            topics(topics.telemetry)
            metrics = telemetryMetrics
            commitIntervalMs = 50
            commitRecordsThreshold = 1
            handle { telemetryHandled += it.value() }
            trafficWarmup(context, "telemetry") {
                mapTopic(topics.telemetry, topics.warmupTelemetry) {
                    quotaProfile = "1 -> (0s, step) -> 300"
                }
                warmupConsumerProperties = warmupConsumerConnection()
                forwardingProducerProperties = forwardingConnection(LongSerializer::class.java)
            }
        }
        return TypedApplication(
            order, batch, telemetry,
            ordersHandled, batchesHandled, telemetryHandled,
            orderMetrics, batchMetrics, telemetryMetrics
        )
    }

    protected fun stringConsumer(
        context: TrafficWarmupContext,
        participantId: String,
        sourceTopic: String,
        warmupTopic: String,
        groupId: String,
        handled: MutableList<String>,
        mainProperties: Map<String, Any?> = consumerProperties(groupId, StringDeserializer::class.java),
        mainRecoveryTimeout: kotlin.time.Duration? = null,
        warmupProperties: Map<String, Any?> = warmupConsumerConnection(),
        beforeHandle: suspend (String) -> Unit = {},
        forwarded: MutableList<TrafficWarmupForwardedEvent>? = null,
        consumerMetrics: ConsumerMetrics<String, String> = object : ConsumerMetrics<String, String> {},
        unorderedWorkers: Int? = null
    ): CoroutinesKafkaConsumer<String, String> = coroutinesKafkaConsumer(
        mainProperties
    ) {
        topics(sourceTopic)
        commitIntervalMs = 50
        commitRecordsThreshold = 1
        metrics = consumerMetrics
        if (unorderedWorkers != null) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            workerConcurrency = unorderedWorkers
        }
        mainRecoveryTimeout?.let { recoveryTimeout = it }
        handle {
            beforeHandle(it.value())
            handled += it.value()
        }
        trafficWarmup(context, participantId) {
            mapTopic(sourceTopic, warmupTopic)
            warmupConsumerProperties = warmupProperties
            forwardingProducerProperties = warmupProperties + mapOf(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
            )
            forwarded?.let { events -> onForwarded(events::add) }
        }
    }

    protected fun context(
        namespace: String,
        controlTopic: String,
        participantStartTimeout: kotlin.time.Duration = 2.seconds,
        warmupDuration: kotlin.time.Duration = 750.milliseconds,
        brokerLifecycleTopic: String? = null,
        brokerLifecycleConnection: Map<String, Any?> = kafkaConnection(),
        delegationTimeout: kotlin.time.Duration = 3.seconds,
        completionTimeout: kotlin.time.Duration = 10.seconds,
        lifecycleObserver: (TrafficWarmupLifecycleEvent) -> Unit = {}
    ) = trafficWarmupContext {
        this.namespace = namespace
        this.controlTopic = controlTopic
        controlProducerProperties = kafkaConnection()
        controlConsumerProperties = kafkaConnection()
        this.warmupDuration = warmupDuration
        offerWindow = 250.milliseconds
        this.participantStartTimeout = participantStartTimeout
        preparationTimeout = 3.seconds
        this.completionTimeout = completionTimeout
        this.delegationTimeout = delegationTimeout
        maxDonorSessions = 4
        maxAggregateQuotaPerSecond = 1_000
        if (brokerLifecycleTopic != null) {
            brokerLifecycle {
                topic = brokerLifecycleTopic
                kafkaProperties = brokerLifecycleConnection
            }
        }
        onLifecycleEvent(lifecycleObserver)
    }

    protected data class TypedApplication(
        val order: CoroutinesKafkaConsumer<String, String>,
        val batch: CoroutinesKafkaConsumer<String, ByteArray>,
        val telemetry: CoroutinesKafkaConsumer<String, Long>,
        val ordersHandled: CopyOnWriteArrayList<String>,
        val batchesHandled: CopyOnWriteArrayList<ByteArray>,
        val telemetryHandled: CopyOnWriteArrayList<Long>,
        val orderMetrics: RecordingWarmupMetrics<String, String>,
        val batchMetrics: RecordingWarmupMetrics<String, ByteArray>,
        val telemetryMetrics: RecordingWarmupMetrics<String, Long>
    ) {
        private val consumers: ConsumerContainer = consumerContainer(order, batch, telemetry)

        fun handledCounts() = listOf(ordersHandled.size, batchesHandled.size, telemetryHandled.size)

        fun startAll() {
            consumers.start()
        }

        suspend fun awaitAllReady() {
            withTimeout(15.seconds) { consumers.awaitReady() }
        }

        suspend fun stopAll() = consumers.stop()
    }

    protected fun consumerProperties(groupId: String, valueDeserializer: Class<*>): Map<String, Any?> =
        kafkaConnection() + mapOf(
            ConsumerConfig.GROUP_ID_CONFIG to groupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to valueDeserializer
        )

    protected fun warmupConsumerConnection(): Map<String, Any?> = kafkaConnection()

    protected fun forwardingConnection(valueSerializer: Class<*>): Map<String, Any?> =
        kafkaConnection() + mapOf(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to valueSerializer
        )

    protected fun kafkaConnection(): Map<String, Any?> =
        mapOf("bootstrap.servers" to kafka.bootstrapServers)

    protected fun <V> produce(
        topic: String,
        key: String,
        value: V,
        serializer: org.apache.kafka.common.serialization.Serializer<V>
    ): RecordMetadata = KafkaProducer(kafkaConnection(), StringSerializer(), serializer).use { producer ->
        producer.send(ProducerRecord(topic, key, value)).get(10, TimeUnit.SECONDS)
    }

    protected fun <V> produceMany(
        topic: String,
        count: Int,
        serializer: org.apache.kafka.common.serialization.Serializer<V>,
        value: (Int) -> V
    ) {
        KafkaProducer(kafkaConnection(), StringSerializer(), serializer).use { producer ->
            val futures = List(count) { index ->
                producer.send(ProducerRecord(topic, "key-$index", value(index)))
            }
            futures.forEach { it.get(10, TimeUnit.SECONDS) }
        }
    }

    protected suspend fun awaitFor(assertion: () -> Boolean) {
        withTimeout(10.seconds) {
            while (!assertion()) kotlinx.coroutines.delay(20)
        }
    }

    protected data class TypedTopics(
        val orders: String,
        val batches: String,
        val telemetry: String,
        val warmupOrders: String,
        val warmupBatches: String,
        val warmupTelemetry: String,
        val control: String
    )

    protected fun unique(prefix: String): String = "$prefix-${UUID.randomUUID()}"

    protected fun createTopics(vararg topics: String) {
        createTopicsWithPartitions(topics.associateWith { 1 })
    }

    protected fun createTopicsWithPartitions(topics: Map<String, Int>) {
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
            admin.createTopics(topics.map { (topic, partitions) ->
                NewTopic(topic, partitions, 1.toShort())
            }).all().get(10, TimeUnit.SECONDS)
        }
    }

    protected fun createTopicsAt(connection: Map<String, Any?>, vararg topics: String) {
        AdminClient.create(connection).use { admin ->
            admin.createTopics(topics.map { topic ->
                NewTopic(topic, 1, 1.toShort())
            }).all().get(10, TimeUnit.SECONDS)
        }
    }

    protected fun produceForeignControlHistory(
        connection: Map<String, Any?>,
        controlTopic: String,
        sourceTopic: String,
        warmupTopic: String,
        count: Int
    ) {
        val mapping = WarmupTopicMapping(
            WarmupParticipantId("foreign-participant"),
            sourceTopic,
            warmupTopic,
            "foreign-mapping-fingerprint"
        )
        val now = System.currentTimeMillis()
        KafkaProducer(connection, StringSerializer(), ByteArraySerializer()).use { producer ->
            repeat(count) { index ->
                val sessionId = WarmupSessionId("foreign-$index")
                val message = WarmupRequested(
                    namespace = WarmupControlNamespace("foreign-v6-namespace"),
                    sessionId = sessionId,
                    senderInstanceId = WarmupInstanceId("foreign-sender"),
                    mappings = listOf(mapping),
                    offerDeadlineEpochMillis = now + 60_000,
                    admissionBudgetPerSecond = normalizedAdmissionBudget(listOf(mapping)),
                    configurationFingerprint = "foreign-configuration-fingerprint",
                    expiresAtEpochMillis = now + 120_000
                )
                producer.send(
                    ProducerRecord(controlTopic, sessionId.value, WarmupControlCodec.encode(message))
                ).get(5, TimeUnit.SECONDS)
            }
        }
    }

    protected fun createTypedTopics(invalidTelemetryDestination: Boolean = false): TypedTopics {
        val topics = TypedTopics(
            unique("orders"), unique("batches"), unique("telemetry"),
            unique("warmup-orders"), unique("warmup-batches"), unique("warmup-telemetry"),
            unique("control")
        )
        createTopicsWithPartitions(
            mapOf(
                topics.orders to 1,
                topics.batches to 1,
                topics.telemetry to 1,
                topics.warmupOrders to 1,
                topics.warmupBatches to 1,
                topics.warmupTelemetry to if (invalidTelemetryDestination) 2 else 1,
                topics.control to 1
            )
        )
        return topics
    }

    companion object {
        @Container
        @JvmStatic
        val sharedKafka = KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"))
    }
}
