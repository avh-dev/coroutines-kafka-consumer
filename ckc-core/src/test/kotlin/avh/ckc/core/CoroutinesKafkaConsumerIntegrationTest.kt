package avh.ckc.core

import avh.ckc.core.polling.partition.offset.OffsetTracker
import avh.ckc.core.polling.partition.offset.OffsetTrackerMetadata
import avh.ckc.core.polling.partition.offset.OffsetTrackerMetadataContext
import avh.ckc.core.warmup.WarmupConsumerId
import avh.ckc.core.warmup.WarmupForwardingAcknowledgement
import avh.ckc.core.warmup.WarmupForwardingConfiguration
import avh.ckc.core.warmup.WarmupForwardingObserver
import avh.ckc.core.warmup.WarmupForwardingProfile
import avh.ckc.core.warmup.WarmupPartitionAssignment
import avh.ckc.core.warmup.WarmupAssignmentConfiguration
import avh.ckc.core.warmup.WarmupSessionId
import avh.ckc.core.warmup.WarmupControlCodec
import avh.ckc.core.warmup.WarmupControlMessage
import avh.ckc.core.warmup.WarmupControlTransport
import avh.ckc.core.warmup.WarmupControlTransportConfiguration
import avh.ckc.core.warmup.WarmupConsumerDefinition
import avh.ckc.core.warmup.WarmupCoordinator
import avh.ckc.core.warmup.WarmupCoordinatorConfiguration
import avh.ckc.core.warmup.WarmupStartupResult
import avh.ckc.core.warmup.WarmupInvalidControlRecordHandler
import avh.ckc.core.warmup.WarmupInstanceId
import avh.ckc.core.warmup.WarmupParticipantConfiguration
import avh.ckc.core.warmup.WarmupRequest
import avh.ckc.core.warmup.WarmupTopicMapping
import avh.ckc.core.warmup.SessionFailed
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.admin.RecordsToDelete
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerInterceptor
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.io.IOException
import java.net.ServerSocket
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern
import kotlin.time.Duration.Companion.milliseconds

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class CoroutinesKafkaConsumerIntegrationTest {

    @Test
    fun `automatic coordinator warms a new instance and starts its main subscription`() = runBlocking {
        val sourceTopic = "coordinator-source-${UUID.randomUUID()}"
        val warmupTopic = "coordinator-warmup-${UUID.randomUUID()}"
        val controlTopic = "coordinator-control-${UUID.randomUUID()}"
        val groupId = "coordinator-main-${UUID.randomUUID()}"
        createTopic(sourceTopic)
        createTopic(warmupTopic)
        createTopic(controlTopic)
        val donorProcessed = AtomicInteger()
        val recipientWarmupProcessed = AtomicInteger()
        val participantConfiguration = WarmupParticipantConfiguration(
            consumerId = WarmupConsumerId("orders"),
            topicMappings = listOf(WarmupTopicMapping(sourceTopic, warmupTopic)),
            forwardingProfile = WarmupForwardingProfile(
                initialFraction = 1.0,
                targetFraction = 1.0,
                rampUpDuration = 0.milliseconds,
                holdDuration = 800.milliseconds
            )
        )
        val forwardingProperties = mapOf<String, Any?>(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.ACKS_CONFIG to "all"
        )
        fun definition(counter: AtomicInteger, countOnlyWarmup: Boolean) = WarmupConsumerDefinition<String, String>(
            configuration = participantConfiguration,
            mainConsumerProperties = consumerProperties(groupId),
            warmupConsumerProperties = consumerProperties("$groupId-warmup"),
            forwardingProducerProperties = forwardingProperties
        ) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            workerConcurrency = 4
            consumerPollLoopConcurrency = 1
            commitIntervalMs = 50
            commitRecordsThreshold = 1
            handle { record ->
                if (!countOnlyWarmup || record.topic() == warmupTopic) counter.incrementAndGet()
            }
        }
        val assignmentConfiguration = WarmupAssignmentConfiguration(
            redisUri = "redis://${redis.host}:${redis.getMappedPort(REDIS_PORT)}",
            lockKey = "ckc:test:coordinator:${UUID.randomUUID()}"
        )
        fun coordinator(instance: String) = WarmupCoordinator(
            WarmupCoordinatorConfiguration(
                instanceId = WarmupInstanceId(instance),
                assignmentConfiguration = assignmentConfiguration,
                donorSearchTimeout = 200.milliseconds,
                stageTimeout = 15_000.milliseconds,
                donorLoadScore = { donorProcessed.get().toDouble() }
            ),
            WarmupControlTransportConfiguration(
                instanceId = WarmupInstanceId(instance),
                topic = controlTopic,
                producerProperties = mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers),
                consumerProperties = mapOf(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers)
            )
        )
        val donor = coordinator("donor-${UUID.randomUUID()}").also {
            it.register(definition(donorProcessed, countOnlyWarmup = false))
        }
        val recipient = coordinator("recipient-${UUID.randomUUID()}").also {
            it.register(definition(recipientWarmupProcessed, countOnlyWarmup = true))
        }

        try {
            assertEquals(WarmupStartupResult.NO_DONOR_FALLBACK, withTimeout(15_000) { donor.start() })
            produce(sourceTopic, "initial", "initial")
            awaitForBlocking { donorProcessed.get().takeIf { it > 0 } }
            val traffic = launch(Dispatchers.IO) {
                KafkaProducer<String, String>(forwardingProperties).use { producer ->
                    repeat(200) { index ->
                        producer.send(ProducerRecord(sourceTopic, "key-$index", "value-$index")).get()
                        delay(10)
                    }
                }
            }
            assertEquals(WarmupStartupResult.WARMED, withTimeout(20_000) { recipient.start() })
            traffic.join()
            assertTrue(recipientWarmupProcessed.get() > 0)
        } finally {
            recipient.stop()
            donor.stop()
        }
    }

    @Test
    fun `control transports establish end positions before sending and each instance receives the stream`() = runBlocking {
        val topic = "warmup-control-${UUID.randomUUID()}"
        createTopic(topic)
        val oldSession = WarmupSessionId("old-${UUID.randomUUID()}")
        val firstId = WarmupInstanceId("first-${UUID.randomUUID()}")
        val secondId = WarmupInstanceId("second-${UUID.randomUUID()}")
        produceControlRecord(
            topic,
            oldSession.value,
            WarmupControlCodec.encode(SessionFailed(oldSession, firstId, null, "old record"))
        )
        val firstMessages = Channel<WarmupControlMessage>(Channel.UNLIMITED)
        val secondMessages = Channel<WarmupControlMessage>(Channel.UNLIMITED)
        val invalidRecords = Channel<String?>(Channel.UNLIMITED)
        fun transport(
            instanceId: WarmupInstanceId,
            messages: Channel<WarmupControlMessage>,
            invalidHandler: WarmupInvalidControlRecordHandler = WarmupInvalidControlRecordHandler.NOOP
        ) =
            WarmupControlTransport(
                WarmupControlTransportConfiguration(
                    instanceId = instanceId,
                    topic = topic,
                    producerProperties = mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers),
                    consumerProperties = mapOf(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers)
                ),
                handler = { messages.send(it) },
                invalidRecordHandler = invalidHandler
            )
        val first = transport(firstId, firstMessages) { key, _ -> invalidRecords.trySend(key) }
        val second = transport(secondId, secondMessages)
        try {
            first.start()
            second.start()
            withTimeout(15_000L) {
                first.awaitReady()
                second.awaitReady()
            }
            produceControlRecord(topic, "malformed", byteArrayOf(1, 2, 3))
            assertEquals("malformed", withTimeout(15_000L) { invalidRecords.receive() })
            val session = WarmupSessionId("session-${UUID.randomUUID()}")
            first.send(WarmupRequest(
                sessionId = session,
                senderInstanceId = firstId,
                participants = listOf(WarmupParticipantConfiguration(
                    consumerId = WarmupConsumerId("orders"),
                    topicMappings = listOf(WarmupTopicMapping("orders", "warmup-orders"))
                ))
            ))

            val receivedByFirst = withTimeout(15_000L) { firstMessages.receive() }
            val receivedBySecond = withTimeout(15_000L) { secondMessages.receive() }
            assertEquals(session, receivedByFirst.sessionId)
            assertEquals(session, receivedBySecond.sessionId)
            assertTrue(firstMessages.tryReceive().isFailure)
            assertTrue(secondMessages.tryReceive().isFailure)
        } finally {
            first.stop()
            second.stop()
        }
    }

    @Test
    fun `donor forwards then completes and recipient drains final committed boundary`() = runBlocking {
        val sourceTopic = "forward-source-${UUID.randomUUID()}"
        val warmupTopic = "forward-target-${UUID.randomUUID()}"
        val donorGroup = "ckc-it-donor-${UUID.randomUUID()}"
        val recipientGroup = "ckc-it-recipient-${UUID.randomUUID()}"
        createTopic(sourceTopic)
        createTopic(warmupTopic)

        val recipientProcessed = CompletableDeferred<Triple<String?, String?, Long>>()
        val forwardingAcknowledged = CompletableDeferred<WarmupForwardingAcknowledgement>()
        val donorHandlerCalls = AtomicInteger()
        val recipient = coroutinesKafkaConsumer<String, String>(consumerProperties(recipientGroup)) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            commitIntervalMs = 100L
            commitRecordsThreshold = 1
            assignedPartitionsAtEnd(TopicPartition(warmupTopic, 0))
            handle { record ->
                recipientProcessed.complete(Triple(record.key(), record.value(), record.offset()))
            }
        }
        val donor = coroutinesKafkaConsumer<String, String>(consumerProperties(donorGroup)) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            commitIntervalMs = 100L
            commitRecordsThreshold = 1
            topics(sourceTopic)
            warmupForwarding(
                WarmupForwardingConfiguration(
                    sessionId = WarmupSessionId("session-1"),
                    consumerId = WarmupConsumerId("orders"),
                    assignments = listOf(
                        WarmupPartitionAssignment(
                            consumerId = WarmupConsumerId("orders"),
                            sourceTopic = sourceTopic,
                            warmupTopic = warmupTopic,
                            warmupPartition = 0
                        )
                    ),
                    profile = WarmupForwardingProfile(
                        initialFraction = 1.0,
                        targetFraction = 1.0,
                        rampUpDuration = kotlin.time.Duration.ZERO
                    ),
                    producerProperties = mapOf(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                        ProducerConfig.ACKS_CONFIG to "all"
                    )
                ),
                WarmupForwardingObserver { forwardingAcknowledged.complete(it) }
            )
            handle { donorHandlerCalls.incrementAndGet() }
        }

        try {
            recipient.start()
            withTimeout(15_000L) { recipient.awaitReady() }
            donor.start()
            withTimeout(15_000L) { donor.awaitReady() }
            produce(sourceTopic, "key-1", "payload-1")

            val acknowledgement = withTimeout(15_000L) { forwardingAcknowledged.await() }
            assertEquals(TopicPartition(sourceTopic, 0), acknowledgement.sourceTopicPartition)
            assertEquals(0L, acknowledgement.sourceOffset)
            assertEquals(TopicPartition(warmupTopic, 0), acknowledgement.warmupTopicPartition)
            assertEquals(
                Triple("key-1", "payload-1", acknowledgement.warmupOffset),
                withTimeout(15_000L) { recipientProcessed.await() }
            )
            awaitFor(timeoutMillis = 15_000L, pauseMillis = 50L) {
                committedOffset(donorGroup, sourceTopic)?.takeIf { it.offset() == 1L }
            }
            assertEquals(0, donorHandlerCalls.get())
            val boundary = donor.completeWarmupForwarding()
            assertEquals(acknowledgement.warmupOffset, boundary.offsets[TopicPartition(warmupTopic, 0)])
            recipient.drainWarmupAndStop(boundary)
            assertEquals(acknowledgement.warmupOffset + 1, committedOffset(recipientGroup, warmupTopic)?.offset())
            produce(sourceTopic, "key-2", "payload-2")
            awaitFor(timeoutMillis = 15_000L, pauseMillis = 50L) {
                donorHandlerCalls.get().takeIf { it == 1 }
            }
        } finally {
            donor.stop()
            recipient.stop()
        }
        Unit
    }

    @Test
    fun `assigned warm-up consumer starts at end and processes only records published after readiness`() = runBlocking {
        val topic = "assigned-warmup-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)
        produce(topic, "old-key", "old-payload")

        val processed = CompletableDeferred<Triple<String?, String?, Long>>()
        val consumer = coroutinesKafkaConsumer<String, String>(consumerProperties(groupId)) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            assignedPartitionsAtEnd(TopicPartition(topic, 0))
            handle { record ->
                processed.complete(Triple(record.key(), record.value(), record.offset()))
            }
        }

        try {
            consumer.start()
            withTimeout(15_000) { consumer.awaitReady() }
            produce(topic, "new-key", "new-payload")

            assertEquals(
                Triple("new-key", "new-payload", 1L),
                withTimeout(15_000) { processed.await() }
            )
        } finally {
            consumer.stop()
        }
    }

    @Test
    fun `when record is produced to real kafka then consumer processes it`() = runBlocking {
        val topic = "ckc-it-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)

        val processed = CompletableDeferred<Triple<String?, String?, Long>>()
        val consumer = coroutinesKafkaConsumer<String, String>(
            consumerProperties = consumerProperties(groupId)
        ) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            topics(topic)
            handle { record ->
                processed.complete(Triple(record.key(), record.value(), record.offset()))
            }
        }

        try {
            consumer.start()
            produce(topic, "key-1", "payload-1")

            assertEquals(
                Triple("key-1", "payload-1", 0L),
                withTimeout(15_000) { processed.await() }
            )
        } finally {
            consumer.stop()
        }
    }

    @Test
    fun `when equal byte array keys are deserialized separately then key ordering remains sequential`() = runBlocking {
        val topic = "byte-array-key-ordering-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)
        produce(topic, byteArrayOf(1, 2, 3), "first")
        produce(topic, byteArrayOf(1, 2, 3), "second")

        val firstStarted = CompletableDeferred<Unit>()
        val secondStarted = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        val consumer = coroutinesKafkaConsumer<ByteArray, String>(
            consumerProperties(groupId) + mapOf(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java
            )
        ) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING
            workerConcurrency = 2
            workChannelCapacity = 8
            topics(topic)
            handle { record ->
                if (record.offset() == 0L) {
                    firstStarted.complete(Unit)
                    releaseFirst.await()
                } else if (record.offset() == 1L) {
                    secondStarted.complete(Unit)
                }
            }
        }

        try {
            consumer.start()
            withTimeout(15_000) { firstStarted.await() }
            delay(500)
            assertFalse(secondStarted.isCompleted)
            releaseFirst.complete(Unit)
            withTimeout(15_000) { secondStarted.await() }
        } finally {
            releaseFirst.complete(Unit)
            consumer.stop()
        }
    }

    @Test
    fun `when single broker is paused then active consumer resumes commits and polling after unpause`() = runBlocking {
        val topic = "broker-pause-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)

        val firstStarted = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        val processedOffsets = CopyOnWriteArrayList<Long>()
        val metrics = RecordingMetrics<String, String>()
        val consumer = coroutinesKafkaConsumer<String, String>(
            consumerProperties(groupId) + mapOf(
                ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG to "1000"
            )
        ) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            commitIntervalMs = 100L
            commitRecordsThreshold = 1
            workerConcurrency = 1
            workChannelCapacity = 8
            this.metrics = metrics
            topics(topic)
            handle { record ->
                if (record.offset() == 0L) {
                    firstStarted.complete(Unit)
                    releaseFirst.await()
                }
                processedOffsets += record.offset()
            }
        }

        var brokerPaused = false
        try {
            consumer.start()
            produce(topic, "key-0", "value-0")
            withTimeout(15_000) { firstStarted.await() }

            kafka.dockerClient.pauseContainerCmd(kafka.containerId).exec()
            brokerPaused = true
            releaseFirst.complete(Unit)
            awaitFor(timeoutMillis = 10_000, pauseMillis = 50) {
                metrics.commits.firstOrNull { !it.success }
            }

            kafka.dockerClient.unpauseContainerCmd(kafka.containerId).exec()
            brokerPaused = false
            awaitFor(timeoutMillis = 20_000, pauseMillis = 50) {
                committedOffset(groupId, topic)?.takeIf { it.offset() == 1L }
            }

            produce(topic, "key-1", "value-1")
            awaitFor(timeoutMillis = 20_000, pauseMillis = 50) {
                processedOffsets.takeIf { it.contains(1L) }
            }
            awaitFor(timeoutMillis = 20_000, pauseMillis = 50) {
                committedOffset(groupId, topic)?.takeIf { it.offset() == 2L }
            }

            assertEquals(listOf(0L, 1L), processedOffsets.toList())
            assertTrue(metrics.commits.any { !it.success })
            assertTrue(metrics.commits.any { it.success })
            assertFalse(consumer.stateSnapshot().failed)
        } finally {
            releaseFirst.complete(Unit)
            if (brokerPaused) {
                kafka.dockerClient.unpauseContainerCmd(kafka.containerId).exec()
            }
            consumer.stop()
        }
    }

    @Test
    fun `when single broker is stopped then active consumer recovers after same broker restarts`() = runBlocking {
        verifyBrokerRestartRecovery("broker-restart") {
            kafka.dockerClient.stopContainerCmd(kafka.containerId).withTimeout(10).exec()
        }
    }

    @Test
    fun `when single broker is killed then active consumer recovers after same broker restarts`() = runBlocking {
        verifyBrokerRestartRecovery("broker-kill") {
            kafka.dockerClient.killContainerCmd(kafka.containerId).exec()
        }
    }

    private suspend fun verifyBrokerRestartRecovery(topicPrefix: String, stopBroker: () -> Unit) {
        val topic = "$topicPrefix-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)

        val firstStarted = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        val processedOffsets = CopyOnWriteArrayList<Long>()
        val metrics = RecordingMetrics<String, String>()
        val consumer = coroutinesKafkaConsumer<String, String>(
            consumerProperties(groupId) + mapOf(
                ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG to "1000"
            )
        ) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            commitIntervalMs = 100L
            commitRecordsThreshold = 1
            workerConcurrency = 1
            workChannelCapacity = 8
            this.metrics = metrics
            topics(topic)
            handle { record ->
                if (record.offset() == 0L) {
                    firstStarted.complete(Unit)
                    releaseFirst.await()
                }
                processedOffsets += record.offset()
            }
        }

        var brokerStopped = false
        try {
            consumer.start()
            produce(topic, "key-0", "value-0")
            withTimeout(15_000) { firstStarted.await() }

            stopBroker()
            brokerStopped = true
            releaseFirst.complete(Unit)
            awaitFor(timeoutMillis = 10_000, pauseMillis = 50) {
                metrics.commits.firstOrNull { !it.success }
            }

            ensureBrokerRunning()
            brokerStopped = false
            awaitFor(timeoutMillis = 20_000, pauseMillis = 50) {
                committedOffset(groupId, topic)?.takeIf { it.offset() == 1L }
            }

            produce(topic, "key-1", "value-1")
            awaitFor(timeoutMillis = 20_000, pauseMillis = 50) {
                processedOffsets.takeIf { it.contains(1L) }
            }
            awaitFor(timeoutMillis = 20_000, pauseMillis = 50) {
                committedOffset(groupId, topic)?.takeIf { it.offset() == 2L }
            }

            assertEquals(listOf(0L, 1L), processedOffsets.toList())
            assertTrue(metrics.commits.any { !it.success })
            assertTrue(metrics.commits.any { it.success })
            assertFalse(consumer.stateSnapshot().failed)
        } finally {
            releaseFirst.complete(Unit)
            if (brokerStopped) {
                ensureBrokerRunning()
            }
            consumer.stop()
        }
    }

    @Test
    fun `when processing mode is FRESHNESS_FIRST_DROP_OLDEST then consumer processes produced record`() = runBlocking {
        val topic = "FRESHNESS_FIRST_DROP_OLDEST-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)

        val processed = CompletableDeferred<String?>()
        val consumer = coroutinesKafkaConsumer<String, String>(consumerProperties(groupId)) {
            processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST
            commitIntervalMs = 100L
            commitRecordsThreshold = 1
            topics(topic)
            handle { record ->
                processed.complete(record.value())
            }
        }

        try {
            consumer.start()
            produce(topic, "FRESHNESS_FIRST_DROP_OLDEST-key", "FRESHNESS_FIRST_DROP_OLDEST-payload")

            assertEquals("FRESHNESS_FIRST_DROP_OLDEST-payload", withTimeout(15_000) { processed.await() })
            awaitFor(timeoutMillis = 15_000, pauseMillis = 50) {
                committedOffset(groupId, topic)?.takeIf { it.offset() == 1L }
            }
        } finally {
            consumer.stop()
        }
    }

    @Test
    fun `when FRESHNESS_FIRST_DROP_OLDEST consumer receives burst then it stays live and processes recent records`() = runBlocking {
        val topic = "FRESHNESS_FIRST_DROP_OLDEST-burst-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)

        val metrics = RecordingMetrics<String, String>()
        val processed = CopyOnWriteArrayList<String>()
        val consumer = coroutinesKafkaConsumer<String, String>(consumerProperties(groupId)) {
            processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST
            workerConcurrency = 1
            workChannelCapacity = 1
            commitIntervalMs = 100L
            commitRecordsThreshold = 1
            this.metrics = metrics
            topics(topic)
            handle { record ->
                delay(100)
                processed += record.value()!!
            }
        }

        try {
            consumer.start()
            repeat(20) { index ->
                produce(topic, "FRESHNESS_FIRST_DROP_OLDEST-key-$index", "FRESHNESS_FIRST_DROP_OLDEST-value-$index")
            }

            awaitFor(timeoutMillis = 20_000, pauseMillis = 50) {
                processed.takeIf { it.isNotEmpty() }
            }
            awaitFor(timeoutMillis = 20_000, pauseMillis = 50) {
                committedOffset(groupId, topic)?.takeIf { it.offset() == 20L }
            }

            assertFalse(processed.isEmpty())
            assertFalse(metrics.polls.isEmpty())
            assertFalse(metrics.processed.isEmpty())
        } finally {
            consumer.stop()
        }
    }

    @Test
    fun `when consumer subscribes by topic pattern then it processes matching topic records`() = runBlocking {
        val topic = "orders-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)

        val processed = CompletableDeferred<Pair<String?, String?>>()
        val consumer = coroutinesKafkaConsumer<String, String>(
            consumerProperties = consumerProperties(groupId)
        ) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            topicsPattern(Pattern.compile("orders-.*"))
            handle { record ->
                processed.complete(record.key() to record.value())
            }
        }

        try {
            consumer.start()
            produce(topic, "order-key", "order-payload")

            assertEquals(
                "order-key" to "order-payload",
                withTimeout(15_000) { processed.await() }
            )
        } finally {
            consumer.stop()
        }
    }

    @Test
    fun `when consumer subscribes by topic pattern then it ignores non matching topics`() = runBlocking {
        val matchingTopic = "orders-${UUID.randomUUID()}"
        val otherTopic = "payments-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(matchingTopic)
        createTopic(otherTopic)

        val processed = CopyOnWriteArrayList<String>()
        val consumer = coroutinesKafkaConsumer<String, String>(
            consumerProperties = consumerProperties(groupId)
        ) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            topicsPattern(Pattern.compile("orders-.*"))
            handle { record ->
                processed += record.value()!!
            }
        }

        try {
            consumer.start()
            delay(1_000)
            produce(otherTopic, "payment-key", "payment-payload")
            produce(matchingTopic, "order-key", "order-payload")

            awaitFor(timeoutMillis = 15_000, pauseMillis = 50) {
                processed.takeIf { it.contains("order-payload") }
            }

            assertFalse(processed.contains("payment-payload"))
            assertFalse(processed.filter { it == "order-payload" }.isEmpty())
        } finally {
            consumer.stop()
        }
    }

    @Test
    fun `when handler fails then processing failure handler receives record on real kafka`() = runBlocking {
        val topic = "failure-handler-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)

        val recovered = CompletableDeferred<Pair<String?, String?>>()
        val consumer = coroutinesKafkaConsumer<String, String>(
            consumerProperties = consumerProperties(groupId)
        ) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            topics(topic)
            onProcessingFailure { record, error ->
                recovered.complete("${record.key()}:${error.message}" to record.value())
            }
            handle {
                throw IllegalStateException("boom")
            }
        }

        try {
            consumer.start()
            produce(topic, "failure-key", "failure-payload")

            assertEquals(
                "failure-key:boom" to "failure-payload",
                withTimeout(15_000) { recovered.await() }
            )
        } finally {
            consumer.stop()
        }
    }

    @Test
    fun `when handler fails on real kafka then metrics record failed outcome`() = runBlocking {
        val topic = "failure-metrics-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)

        val metrics = RecordingMetrics<String, String>()
        val recovered = CompletableDeferred<Unit>()
        val consumer = coroutinesKafkaConsumer<String, String>(
            consumerProperties = consumerProperties(groupId)
        ) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            this.metrics = metrics
            topics(topic)
            onProcessingFailure { _, _ ->
                recovered.complete(Unit)
            }
            handle {
                throw IllegalStateException("metrics-boom")
            }
        }

        try {
            consumer.start()
            produce(topic, "failure-key", "failure-payload")
            withTimeout(15_000) { recovered.await() }

            assertEquals(1, metrics.failed.size)
            assertEquals("metrics-boom", metrics.failed.single().error.message)
            assertFalse(metrics.polls.isEmpty())
        } finally {
            consumer.stop()
        }
    }

    @Test
    fun `when handler is slow in AT_LEAST_ONCE_NO_ORDERING mode then all produced records are eventually processed`() = runBlocking {
        val topic = "AT_LEAST_ONCE_NO_ORDERING-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)

        val processed = CopyOnWriteArrayList<Long>()
        val consumer = coroutinesKafkaConsumer<String, String>(
            consumerProperties = consumerProperties(groupId) + mapOf(
                "max.poll.records" to "5"
            )
        ) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            workerConcurrency = 1
            workChannelCapacity = 1
            topics(topic)
            handle { record ->
                delay(100)
                processed += record.value()!!.toLong()
            }
        }

        try {
            consumer.start()
            repeat(10) { index ->
                produce(topic, "key-$index", index.toString())
            }

            awaitFor(timeoutMillis = 20_000, pauseMillis = 50) {
                processed.takeIf { it.size == 10 }
            }

            assertEquals((0L..9L).toList(), processed.sorted())
        } finally {
            consumer.stop()
        }
    }

    @Test
    fun `when AT_LEAST_ONCE_NO_ORDERING commits offset then offset metadata is stored in kafka`() = runBlocking {
        val topic = "metadata-commit-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)

        val processed = CompletableDeferred<Long>()
        val consumer = coroutinesKafkaConsumer<String, String>(
            consumerProperties = consumerProperties(groupId)
        ) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            commitIntervalMs = 100L
            topics(topic)
            handle { record ->
                processed.complete(record.offset())
            }
        }

        try {
            consumer.start()
            produce(topic, "metadata-key", "metadata-payload")

            assertEquals(0L, withTimeout(15_000) { processed.await() })
            val committed = awaitFor(timeoutMillis = 15_000, pauseMillis = 50) {
                committedOffset(groupId, topic)?.takeIf {
                    it.offset() == 1L && it.metadata().startsWith(OffsetTrackerMetadata.PREFIX)
                }
            }

            OffsetTrackerMetadata.decode(
                committed.metadata(),
                OffsetTrackerMetadataContext(groupId, TopicPartition(topic, 0), committed.offset())
            )
            Unit
        } finally {
            consumer.stop()
        }
    }

    @Test
    fun `when committed offset metadata contains processed offset then restored consumer skips it`() = runBlocking {
        val topic = "metadata-restore-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)

        produce(topic, "key-0", "0")
        produce(topic, "key-1", "1")
        produce(topic, "key-2", "2")

        val tracker = OffsetTracker(initialProcessedOffset = 0L)
        tracker.markProcessed(1L)
        commitOffset(
            groupId = groupId,
            topic = topic,
            offset = 1L,
            metadata = OffsetTrackerMetadata.encode(
                tracker.snapshot(),
                OffsetTrackerMetadataContext(groupId, TopicPartition(topic, 0), 1L)
            ).metadata!!
        )

        val processed = CopyOnWriteArrayList<Long>()
        val consumer = coroutinesKafkaConsumer<String, String>(
            consumerProperties = consumerProperties(groupId)
        ) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            commitIntervalMs = 100L
            topics(topic)
            handle { record ->
                processed += record.offset()
            }
        }

        try {
            consumer.start()

            awaitFor(timeoutMillis = 15_000, pauseMillis = 50) {
                processed.takeIf { it.contains(2L) }
            }
            delay(500)

            assertFalse(processed.contains(1L))
            assertEquals(listOf(2L), processed.toList())
        } finally {
            consumer.stop()
        }
    }

    @Test
    fun `when group offset is reset backwards then restarted consumer reprocesses from authoritative offset`() = runBlocking {
        val topic = "admin-reset-backwards-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)
        repeat(6) { index -> produce(topic, "key-$index", "$index") }
        consumeUntilCommitted(topic, groupId, expectedOffset = 6L)

        assertFalse(committedOffset(groupId, topic)?.metadata().isNullOrEmpty())
        alterGroupOffset(groupId, topic, offset = 2L)
        val resetOffset = committedOffset(groupId, topic)
        assertEquals(2L, resetOffset?.offset())
        assertTrue(resetOffset?.metadata().isNullOrEmpty())

        val processed = CopyOnWriteArrayList<Long>()
        val consumer = testConsumer(topic, groupId) { record -> processed += record.offset() }
        try {
            consumer.start()
            awaitFor(timeoutMillis = 20_000) {
                processed.takeIf { it.containsAll(listOf(2L, 3L, 4L, 5L)) }
            }
            awaitFor(timeoutMillis = 20_000) {
                committedOffset(groupId, topic)?.takeIf { it.offset() == 6L }
            }

            assertEquals(listOf(2L, 3L, 4L, 5L), processed.sorted())
        } finally {
            consumer.stop()
        }
    }

    @Test
    fun `when backward reset preserves stale CKC metadata then restarted consumer ignores it`() = runBlocking {
        val topic = "stale-metadata-reset-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)
        repeat(6) { index -> produce(topic, "key-$index", "$index") }
        consumeUntilCommitted(topic, groupId, expectedOffset = 6L)

        val staleMetadata = committedOffset(groupId, topic)?.metadata()
        assertTrue(staleMetadata?.startsWith(OffsetTrackerMetadata.PREFIX) == true)
        alterGroupOffset(groupId, topic, offset = 2L, metadata = staleMetadata!!)

        val resetOffset = committedOffset(groupId, topic)
        assertEquals(2L, resetOffset?.offset())
        assertEquals(staleMetadata, resetOffset?.metadata())

        val processed = CopyOnWriteArrayList<Long>()
        val consumer = testConsumer(topic, groupId) { record -> processed += record.offset() }
        try {
            consumer.start()
            awaitFor(timeoutMillis = 20_000) {
                processed.takeIf { it.containsAll(listOf(2L, 3L, 4L, 5L)) }
            }
            awaitFor(timeoutMillis = 20_000) {
                committedOffset(groupId, topic)?.takeIf { it.offset() == 6L }
            }

            assertEquals(listOf(2L, 3L, 4L, 5L), processed.sorted())
        } finally {
            consumer.stop()
        }
    }

    @Test
    fun `when deserialization fails permanently then consumer reports failure`() = runBlocking {
        val topic = "deser-failure-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)

        val metrics = RecordingMetrics<Long, Long>()
        val consumer = coroutinesKafkaConsumer<Long, Long>(
            consumerProperties = mapOf(
                "bootstrap.servers" to kafka.bootstrapServers,
                "group.id" to groupId,
                "auto.offset.reset" to "earliest",
                "enable.auto.commit" to "false",
                "key.deserializer" to LongDeserializer::class.java,
                "value.deserializer" to LongDeserializer::class.java
            )
        ) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            this.metrics = metrics
            topics(topic)
            handle { }
        }

        try {
            consumer.start()
            produce(topic, "bad-key", "not-a-long")

            val failure = withTimeout(15_000) {
                awaitFor(timeoutMillis = 15_000, pauseMillis = 50) {
                    metrics.consumerFailures.firstOrNull()
                }
            }

            assertTrue(failure is org.apache.kafka.common.errors.RecordDeserializationException)
            assertEquals("Size of data received by LongDeserializer is not 8", failure.cause?.message)
            val thrown = try {
                withTimeout(5_000) { consumer.stop() }
                null
            } catch (error: Throwable) {
                error
            }
            assertTrue(thrown is org.apache.kafka.common.errors.RecordDeserializationException)
            assertEquals("Size of data received by LongDeserializer is not 8", thrown?.cause?.message)
        } finally {
            try {
                withTimeout(5_000) { consumer.stop() }
            } catch (_: Throwable) {
            }
        }
    }

    @Test
    fun `when handler fails transiently then retry policy eventually processes the record`() = runBlocking {
        val topic = "retry-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)

        val attempts = AtomicInteger()
        val metrics = RecordingMetrics<String, String>()
        val processed = CompletableDeferred<String?>()
        val consumer = coroutinesKafkaConsumer<String, String>(
            consumerProperties = consumerProperties(groupId)
        ) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            retryPolicy = retryPolicy {
                retry<IOException> {
                    maxRetries = 2
                    delay = 10.milliseconds
                }
            }
            this.metrics = metrics
            topics(topic)
            handle { record ->
                if (attempts.getAndIncrement() < 2) {
                    throw IOException("transient")
                }
                processed.complete(record.value())
            }
        }

        try {
            consumer.start()
            produce(topic, "retry-key", "retry-payload")

            assertEquals("retry-payload", withTimeout(15_000) { processed.await() })
            awaitFor(timeoutMillis = 15_000, pauseMillis = 50) {
                metrics.takeIf {
                    it.retries.map { retry -> retry.attempt } == listOf(1, 2) &&
                            it.processed.size == 1 &&
                            it.polls.isNotEmpty()
                }
            }

            assertEquals(listOf(1, 2), metrics.retries.map { it.attempt })
            assertEquals(1, metrics.processed.size)
            assertFalse(metrics.polls.isEmpty())
        } finally {
            consumer.stop()
        }
    }

    @Test
    fun `when consumer is cancelled before commit then next consumer in same group receives the record again`() = runBlocking {
        val topic = "redelivery-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)

        val started = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val firstConsumerJob = SupervisorJob()
        val firstConsumer = coroutinesKafkaConsumer<String, String>(
            consumerProperties = consumerProperties(groupId)
        ) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            parentContext = firstConsumerJob
            topics(topic)
            handle {
                started.complete(Unit)
                release.await()
            }
        }

        firstConsumer.start()
        produce(topic, "redelivery-key", "redelivery-payload")
        withTimeout(15_000) { started.await() }
        firstConsumerJob.cancel()
        delay(1_000)

        val redelivered = CompletableDeferred<String?>()
        val secondConsumer = coroutinesKafkaConsumer<String, String>(
            consumerProperties = consumerProperties(groupId)
        ) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            topics(topic)
            handle { record ->
                redelivered.complete(record.value())
            }
        }

        try {
            secondConsumer.start()
            release.complete(Unit)
            assertEquals("redelivery-payload", withTimeout(15_000) { redelivered.await() })
        } finally {
            secondConsumer.stop()
        }
    }

    @Test
    fun `when second consumer joins group then records are processed across rebalance`() = runBlocking {
        val topic = "rebalance-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic, partitions = 2)

        val processed = CopyOnWriteArrayList<String>()
        val firstConsumer = coroutinesKafkaConsumer<String, String>(
            consumerProperties = consumerProperties(groupId)
        ) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            workerConcurrency = 1
            topics(topic)
            handle { record ->
                processed += "c1:${record.value()!!}"
                delay(50)
            }
        }

        val secondConsumer = coroutinesKafkaConsumer<String, String>(
            consumerProperties = consumerProperties(groupId)
        ) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            workerConcurrency = 1
            topics(topic)
            handle { record ->
                processed += "c2:${record.value()!!}"
                delay(50)
            }
        }

        try {
            firstConsumer.start()
            repeat(4) { index ->
                produce(topic, "rebalance-key-$index", "before-$index", partition = index % 2)
            }

            awaitFor(timeoutMillis = 15_000, pauseMillis = 50) {
                processed.takeIf { it.size >= 4 }
            }

            secondConsumer.start()
            delay(5_000)
            repeat(4) { index ->
                produce(topic, "rebalance-key-2-$index", "after-$index", partition = index % 2)
            }

            awaitFor(timeoutMillis = 20_000, pauseMillis = 50) {
                processed.takeIf { it.size >= 8 }
            }

            awaitFor(timeoutMillis = 15_000, pauseMillis = 50) {
                processed.takeIf { entries -> entries.any { it.startsWith("c2:") } }
            }

            assertFalse(processed.none { it.startsWith("c1:") })
            assertFalse(processed.none { it.startsWith("c2:") })
        } finally {
            secondConsumer.stop()
            firstConsumer.stop()
        }
    }

    @Test
    fun `when partitions are revoked with active offset holes then handoff drains before reassignment`() = runBlocking {
        val topic = "rebalance-active-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        val partitions = 4
        createTopic(topic, partitions)

        val releaseBlockedRecords = CompletableDeferred<Unit>()
        val firstBlockedPartitions = ConcurrentHashMap.newKeySet<Int>()
        val firstCompleted = ConcurrentHashMap.newKeySet<Pair<Int, Long>>()
        val secondCompleted = ConcurrentHashMap.newKeySet<Pair<Int, Long>>()
        val firstConsumer = activeRebalanceConsumer(topic, groupId) { record ->
            if (record.offset() == 0L) {
                firstBlockedPartitions += record.partition()
                releaseBlockedRecords.await()
            }
            firstCompleted += record.partition() to record.offset()
        }
        val secondConsumer = activeRebalanceConsumer(topic, groupId) { record ->
            secondCompleted += record.partition() to record.offset()
        }

        try {
            firstConsumer.start()
            awaitFor(timeoutMillis = 20_000) {
                firstConsumer.stateSnapshot().takeIf { it.assignedPartitionCount == partitions }
            }

            repeat(partitions) { partition ->
                repeat(3) { offset ->
                    produce(topic, "key-$partition-$offset", "value-$partition-$offset", partition)
                }
            }
            awaitFor(timeoutMillis = 20_000) {
                firstBlockedPartitions.takeIf { it.size == partitions }
            }
            awaitFor(timeoutMillis = 20_000) {
                firstCompleted.takeIf { completed ->
                    (0 until partitions).all { partition ->
                        completed.contains(partition to 1L) && completed.contains(partition to 2L)
                    }
                }
            }

            secondConsumer.start()
            awaitFor(timeoutMillis = 20_000) {
                assignedPartitions(firstConsumer).takeIf { it.size < partitions }
            }
            releaseBlockedRecords.complete(Unit)

            val assignments = awaitFor(timeoutMillis = 30_000) {
                val firstAssigned = assignedPartitions(firstConsumer)
                val secondAssigned = assignedPartitions(secondConsumer)
                (firstAssigned to secondAssigned).takeIf {
                    firstAssigned.isNotEmpty() &&
                            secondAssigned.isNotEmpty() &&
                            firstAssigned.intersect(secondAssigned).isEmpty() &&
                            firstAssigned + secondAssigned == (0 until partitions).toSet()
                }
            }
            val firstAssigned = assignments.first
            val secondAssigned = assignments.second

            awaitFor(timeoutMillis = 20_000) {
                firstCompleted.takeIf { completed ->
                    (0 until partitions).all { completed.contains(it to 0L) }
                }
            }
            awaitFor(timeoutMillis = 20_000) {
                groupOffsets(groupId, topic, partitions).takeIf { offsets ->
                    (0 until partitions).all { (offsets[it] ?: -1L) >= 3L }
                }
            }
            assertTrue(
                secondCompleted.none { (partition, offset) ->
                    partition in secondAssigned && offset in 0L..2L
                }
            )
            assertTrue(firstAssigned.isNotEmpty())
            assertTrue(secondAssigned.isNotEmpty())

            repeat(partitions) { partition ->
                produce(topic, "after-key-$partition", "after-value-$partition", partition)
            }
            awaitFor(timeoutMillis = 20_000) {
                groupOffsets(groupId, topic, partitions).takeIf { offsets ->
                    (0 until partitions).all { (offsets[it] ?: -1L) >= 4L }
                }
            }
        } finally {
            releaseBlockedRecords.complete(Unit)
            secondConsumer.stop()
            firstConsumer.stop()
        }
        Unit
    }

    @Test
    fun `when earliest skips offsets removed from the log then commits advance across the gap`() = runBlocking {
        val topic = "retention-earliest-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)
        produce(topic, "key-0", "0")
        produce(topic, "key-1", "1")
        consumeUntilCommitted(topic, groupId, expectedOffset = 2L)

        repeat(4) { index -> produce(topic, "key-${index + 2}", "${index + 2}") }
        deleteRecordsBefore(topic, 4L)
        assertEquals(2L, committedOffset(groupId, topic)?.offset())
        assertEquals(4L, logStartOffset(topic))

        val processed = CopyOnWriteArrayList<Long>()
        val consumer = testConsumer(topic, groupId, autoOffsetReset = "earliest") { record ->
            processed += record.offset()
        }
        try {
            consumer.start()
            awaitFor(timeoutMillis = 20_000) { processed.takeIf { it.containsAll(listOf(4L, 5L)) } }
            awaitFor(timeoutMillis = 20_000) {
                committedOffset(groupId, topic)?.takeIf { it.offset() > 4L }
            }

            assertEquals(listOf(4L, 5L), processed.sorted())
        } finally {
            consumer.stop()
        }
    }

    @Test
    fun `when latest skips retained records then a new record commits across the gap`() = runBlocking {
        val topic = "retention-latest-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)
        produce(topic, "key-0", "0")
        produce(topic, "key-1", "1")
        consumeUntilCommitted(topic, groupId, expectedOffset = 2L)

        repeat(4) { index -> produce(topic, "key-${index + 2}", "${index + 2}") }
        deleteRecordsBefore(topic, 4L)
        assertEquals(2L, committedOffset(groupId, topic)?.offset())
        assertEquals(4L, logStartOffset(topic))

        val processed = CopyOnWriteArrayList<Long>()
        val consumer = testConsumer(topic, groupId, autoOffsetReset = "latest") { record ->
            processed += record.offset()
        }
        try {
            consumer.start()
            awaitFor(timeoutMillis = 15_000) {
                consumer.stateSnapshot().takeIf { it.assignedPartitionCount == 1 }
            }
            delay(500)
            produce(topic, "key-6", "6")

            awaitFor(timeoutMillis = 20_000) { processed.takeIf { it.contains(6L) } }
            awaitFor(timeoutMillis = 20_000) {
                committedOffset(groupId, topic)?.takeIf { it.offset() == 7L }
            }

            assertEquals(listOf(6L), processed.toList())
        } finally {
            consumer.stop()
        }
    }

    @Test
    fun `when kafka client filters an internal offset then commit advances across the gap`() = runBlocking {
        val topic = "filtered-gap-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)
        repeat(3) { index -> produce(topic, "key-$index", "$index") }

        val processed = CopyOnWriteArrayList<Long>()
        val consumer = testConsumer(
            topic = topic,
            groupId = groupId,
            additionalProperties = mapOf(
                ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG to DropOffsetOneConsumerInterceptor::class.java.name
            )
        ) { record ->
            processed += record.offset()
        }
        try {
            consumer.start()
            awaitFor(timeoutMillis = 20_000) {
                committedOffset(groupId, topic)?.takeIf { it.offset() == 3L }
            }

            assertEquals(listOf(0L, 2L), processed.sorted())
        } finally {
            consumer.stop()
        }
    }

    private fun consumerProperties(groupId: String, autoOffsetReset: String = "earliest"): Map<String, Any?> = mapOf(
        "bootstrap.servers" to kafka.bootstrapServers,
        "group.id" to groupId,
        "auto.offset.reset" to autoOffsetReset,
        "enable.auto.commit" to "false",
        "key.deserializer" to StringDeserializer::class.java,
        "value.deserializer" to StringDeserializer::class.java
    )

    private fun createTopic(topic: String, partitions: Int = 1) {
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
            admin.createTopics(listOf(NewTopic(topic, partitions, 1))).all().get()
        }
    }

    private fun committedOffset(groupId: String, topic: String, partition: Int = 0): OffsetAndMetadata? {
        val topicPartition = TopicPartition(topic, partition)
        KafkaConsumer<String, String>(consumerProperties(groupId)).use { consumer ->
            return consumer.committed(setOf(topicPartition))[topicPartition]
        }
    }

    private fun groupOffsets(groupId: String, topic: String, partitions: Int): Map<Int, Long> {
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
            val offsets = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get()
            return (0 until partitions).associateWith { partition ->
                offsets[TopicPartition(topic, partition)]?.offset() ?: -1L
            }
        }
    }

    private fun assignedPartitions(consumer: CoroutinesKafkaConsumer<String, String>): Set<Int> =
        consumer.stateSnapshot().pollLoops
            .flatMap { it.assignedPartitions }
            .mapTo(mutableSetOf()) { it.partition }

    private fun activeRebalanceConsumer(
        topic: String,
        groupId: String,
        handler: KafkaRecordHandler<String, String>
    ): CoroutinesKafkaConsumer<String, String> =
        coroutinesKafkaConsumer(consumerProperties(groupId)) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            commitIntervalMs = 100L
            commitRecordsThreshold = 1
            workerConcurrency = 8
            workChannelCapacity = 64
            topics(topic)
            handle { record -> handler.process(record) }
        }

    private fun commitOffset(
        groupId: String,
        topic: String,
        partition: Int = 0,
        offset: Long,
        metadata: String
    ) {
        val topicPartition = TopicPartition(topic, partition)
        KafkaConsumer<String, String>(consumerProperties(groupId)).use { consumer ->
            consumer.assign(listOf(topicPartition))
            consumer.commitSync(mapOf(topicPartition to OffsetAndMetadata(offset, metadata)))
        }
    }

    private fun alterGroupOffset(
        groupId: String,
        topic: String,
        offset: Long,
        partition: Int = 0,
        metadata: String = ""
    ) {
        val topicPartition = TopicPartition(topic, partition)
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
            admin.alterConsumerGroupOffsets(
                groupId,
                mapOf(topicPartition to OffsetAndMetadata(offset, metadata))
            ).all().get()
        }
    }

    private suspend fun consumeUntilCommitted(topic: String, groupId: String, expectedOffset: Long) {
        val consumer = testConsumer(topic, groupId) { }
        try {
            consumer.start()
            awaitFor(timeoutMillis = 20_000) {
                committedOffset(groupId, topic)?.takeIf { it.offset() == expectedOffset }
            }
        } finally {
            consumer.stop()
        }
    }

    private fun testConsumer(
        topic: String,
        groupId: String,
        autoOffsetReset: String = "earliest",
        additionalProperties: Map<String, Any?> = emptyMap(),
        handler: KafkaRecordHandler<String, String>
    ): CoroutinesKafkaConsumer<String, String> =
        coroutinesKafkaConsumer(consumerProperties(groupId, autoOffsetReset) + additionalProperties) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            commitIntervalMs = 100L
            commitRecordsThreshold = 1
            workerConcurrency = 4
            workChannelCapacity = 32
            topics(topic)
            handle { record -> handler.process(record) }
        }

    private fun deleteRecordsBefore(topic: String, offset: Long, partition: Int = 0) {
        val topicPartition = TopicPartition(topic, partition)
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
            admin.deleteRecords(mapOf(topicPartition to RecordsToDelete.beforeOffset(offset))).all().get()
        }
        awaitForBlocking {
            logStartOffset(topic, partition).takeIf { it == offset }
        }
    }

    private fun logStartOffset(topic: String, partition: Int = 0): Long {
        val topicPartition = TopicPartition(topic, partition)
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
            return admin.listOffsets(mapOf(topicPartition to OffsetSpec.earliest()))
                .all().get().getValue(topicPartition).offset()
        }
    }

    private fun <T : Any> awaitForBlocking(timeoutMillis: Long = 15_000, block: () -> T?): T {
        val deadline = System.currentTimeMillis() + timeoutMillis
        while (System.currentTimeMillis() < deadline) {
            block()?.let { return it }
            Thread.sleep(50)
        }
        error("Condition was not met within ${timeoutMillis}ms")
    }

    private fun produce(topic: String, key: String, value: String, partition: Int? = null) {
        KafkaProducer<String, String>(
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.ACKS_CONFIG to "all"
            )
        ).use { producer ->
            producer.send(ProducerRecord(topic, partition, key, value)).get()
            producer.flush()
        }
    }

    private fun produceControlRecord(topic: String, key: String, value: ByteArray) {
        KafkaProducer<String, ByteArray>(
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                ProducerConfig.ACKS_CONFIG to "all"
            )
        ).use { producer ->
            producer.send(ProducerRecord(topic, key, value)).get(10, TimeUnit.SECONDS)
        }
    }

    private fun produce(topic: String, key: ByteArray, value: String, partition: Int? = null) {
        KafkaProducer<ByteArray, String>(
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.ACKS_CONFIG to "all"
            )
        ).use { producer ->
            producer.send(ProducerRecord(topic, partition, key, value)).get()
            producer.flush()
        }
    }

    private fun ensureBrokerRunning(timeoutMillis: Long = 30_000) {
        val running = kafka.dockerClient.inspectContainerCmd(kafka.containerId).exec().state.running == true
        if (!running) {
            kafka.dockerClient.startContainerCmd(kafka.containerId).exec()
        }

        val deadline = System.currentTimeMillis() + timeoutMillis
        var lastFailure: Exception? = null
        while (System.currentTimeMillis() < deadline) {
            try {
                AdminClient.create(
                    mapOf(
                        "bootstrap.servers" to kafka.bootstrapServers,
                        "default.api.timeout.ms" to "1000",
                        "request.timeout.ms" to "1000"
                    )
                ).use { admin ->
                    admin.describeCluster().clusterId().get(2, TimeUnit.SECONDS)
                }
                return
            } catch (error: Exception) {
                lastFailure = error
                Thread.sleep(100)
            }
        }
        val state = kafka.dockerClient.inspectContainerCmd(kafka.containerId).exec().state
        val logs = kafka.logs.takeLast(8_000)
        error(
            "Kafka broker did not become ready within ${timeoutMillis}ms; " +
                    "running=${state.running}, status=${state.status}, exitCode=${state.exitCodeLong}, " +
                    "lastFailure=${lastFailure?.javaClass?.name}: ${lastFailure?.message}\n$logs"
        )
    }

    companion object {
        @Container
        @JvmStatic
        val kafka: KafkaContainer = RestartableKafkaContainer(
            DockerImageName.parse("apache/kafka-native:3.8.0")
        ).withFixedKafkaPort(availableTcpPort())

        @Container
        @JvmStatic
        val redis = CoreRedisContainer(DockerImageName.parse("redis:7.4-alpine"))

        const val REDIS_PORT = 6379

        private fun availableTcpPort(): Int = ServerSocket(0).use { it.localPort }
    }
}

class CoreRedisContainer(imageName: DockerImageName) : GenericContainer<CoreRedisContainer>(imageName) {
    init {
        withExposedPorts(6379)
    }
}

private class RestartableKafkaContainer(imageName: DockerImageName) : KafkaContainer(imageName) {
    fun withFixedKafkaPort(hostPort: Int): KafkaContainer =
        apply { addFixedExposedPort(hostPort, KAFKA_PORT) }

    private companion object {
        const val KAFKA_PORT = 9092
    }
}

class DropOffsetOneConsumerInterceptor : ConsumerInterceptor<String, String> {
    override fun configure(configs: MutableMap<String, *>?) = Unit

    override fun onConsume(records: ConsumerRecords<String, String>): ConsumerRecords<String, String> =
        ConsumerRecords(
            records.partitions().associateWith { topicPartition ->
                records.records(topicPartition).filterNot { it.offset() == 1L }
            }
        )

    override fun onCommit(offsets: MutableMap<TopicPartition, OffsetAndMetadata>?) = Unit

    override fun close() = Unit
}
