package avh.ckc.core

import avh.ckc.core.polling.partition.offset.OffsetTracker
import avh.ckc.core.polling.partition.offset.OffsetTrackerMetadata
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
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
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.io.IOException
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern
import kotlin.time.Duration.Companion.milliseconds

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class CoroutinesKafkaConsumerIntegrationTest {

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
    fun `when processing mode is FRESHNESS_FIRST_DROP_OLDEST with auto commit then consumer processes produced record`() = runBlocking {
        val topic = "FRESHNESS_FIRST_DROP_OLDEST-${UUID.randomUUID()}"
        val groupId = "ckc-it-group-${UUID.randomUUID()}"
        createTopic(topic)

        val processed = CompletableDeferred<String?>()
        val consumer = coroutinesKafkaConsumer<String, String>(
            consumerProperties = consumerProperties(groupId) + mapOf(
                "enable.auto.commit" to "true"
            )
        ) {
            processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST
            topics(topic)
            handle { record ->
                processed.complete(record.value())
            }
        }

        try {
            consumer.start()
            produce(topic, "FRESHNESS_FIRST_DROP_OLDEST-key", "FRESHNESS_FIRST_DROP_OLDEST-payload")

            assertEquals("FRESHNESS_FIRST_DROP_OLDEST-payload", withTimeout(15_000) { processed.await() })
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
        val consumer = coroutinesKafkaConsumer<String, String>(
            consumerProperties = consumerProperties(groupId) + mapOf(
                "enable.auto.commit" to "true"
            )
        ) {
            processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST
            workerConcurrency = 1
            workChannelCapacity = 1
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
                    it.offset() == 1L && it.metadata().isNotEmpty()
                }
            }

            OffsetTrackerMetadata.decode(committed.metadata())
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

        val tracker = OffsetTracker(lastCommitedOffset = 0L)
        tracker.markProcessed(1L)
        commitOffset(
            groupId = groupId,
            topic = topic,
            offset = 1L,
            metadata = OffsetTrackerMetadata.encode(tracker.snapshot())!!
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
            val thrown = assertThrows(org.apache.kafka.common.errors.RecordDeserializationException::class.java) {
                runBlocking {
                    consumer.stop()
                }
            }
            assertEquals("Size of data received by LongDeserializer is not 8", thrown.cause?.message)
        } finally {
            try {
                consumer.stop()
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

    private fun consumerProperties(groupId: String): Map<String, Any?> = mapOf(
        "bootstrap.servers" to kafka.bootstrapServers,
        "group.id" to groupId,
        "auto.offset.reset" to "earliest",
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

    companion object {
        @Container
        @JvmStatic
        val kafka = KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"))
    }
}
