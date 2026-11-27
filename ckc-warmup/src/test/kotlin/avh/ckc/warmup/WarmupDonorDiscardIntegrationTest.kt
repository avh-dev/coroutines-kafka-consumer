package avh.ckc.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.ProcessingMode
import avh.ckc.core.consumerContainer
import avh.ckc.core.coroutinesKafkaConsumer
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.metrics.RecordHandoffReason
import avh.ckc.warmup.protocol.ControlRecordCompleted
import avh.ckc.warmup.protocol.WarmupDiscardReason
import avh.ckc.warmup.protocol.WarmupLivenessProbe
import avh.ckc.warmup.protocol.WarmupRemoteOutcome
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.RangeAssignor
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Testcontainers

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
internal class WarmupDonorDiscardIntegrationTest : WarmupContextKafkaFixture() {
    @Test
    fun `at least once donor handles a recipient stale discard and commits the source offset`() = runBlocking {
        withStaleResult { delegation ->
            delegation.releaseStaleResult.complete(Unit)
            awaitFor { delegation.donorHandled == listOf(delegation.offset) }
            delegation.awaitCommit()
            assertEquals(listOf(WarmupResultMetricOutcome.DISCARDED), delegation.metrics.warmupResults.map { it.outcome })
            assertTrue(delegation.metrics.dropped.isEmpty())
        }
    }

    @Test
    fun `freshness donor accepts stale discard once the original record exceeds its age limit`() = runBlocking {
        val donorAgeLimit = 32.seconds
        withStaleResult(donorAgeLimit) { delegation ->
            delay(delegation.timestamp + donorAgeLimit.inWholeMilliseconds + 1 - System.currentTimeMillis())
            delegation.releaseStaleResult.complete(Unit)

            awaitFor { delegation.metrics.dropped.isNotEmpty() }
            assertEquals(
                listOf(delegation.offset to RecordDropReason.STALE_AGE),
                delegation.metrics.dropped.map { it.record.offset() to it.reason }
            )
            assertTrue(delegation.donorHandled.isEmpty())
            assertTrue(delegation.metrics.warmupFallbacks.isEmpty())
            delegation.awaitCommit()
        }
    }

    @Test
    fun `at least once donor hands off an incompatible discard after partition revoke`() = runBlocking {
        withStaleResult { delegation ->
            val nextHandled = CompletableDeferred<Long>()
            val nextOwner = coroutinesKafkaConsumer<String, String>(groupProperties(delegation.groupId, "a-next")) {
                topics(delegation.partition.topic())
                commitRecordsThreshold = 1
                handle { nextHandled.complete(it.offset()) }
            }
            try {
                nextOwner.start()
                // The eager rebalance revokes the lease before waiting for the delegated pipeline.
                awaitFor { delegation.donor.runtimeControl.partitionOwnership(delegation.partition.topic()).isEmpty() }
                delegation.releaseStaleResult.complete(Unit)

                assertEquals(delegation.offset, withTimeout(10.seconds) { nextHandled.await() })
                delegation.awaitCommit()
                assertEquals(
                    listOf(WarmupResultMetricOutcome.DISCARDED),
                    delegation.metrics.warmupResults.map { it.outcome }
                )
                assertEquals(
                    listOf(delegation.offset to RecordHandoffReason.ASSIGNMENT_ENDED),
                    delegation.metrics.handedOff.map { it.record.offset() to it.reason }
                )
                assertTrue(delegation.donorHandled.isEmpty())
                assertTrue(delegation.metrics.dropped.isEmpty())
            } finally {
                delegation.releaseStaleResult.complete(Unit)
                nextOwner.stop()
            }
        }
    }

    @Test
    fun `incompatible discard keeps another result for the same mapping in flight`() = runBlocking {
        withStaleResult { delegation ->
            val freshOffset = produce(delegation.partition.topic(), "fresh", "fresh", StringSerializer()).offset()
            assertEquals(WarmupRemoteOutcome.Succeeded, withTimeout(10.seconds) { delegation.freshResult.await() })

            // The recipient already finished the fresh record. Deliver the stale result first.
            delegation.releaseStaleResult.complete(Unit)
            awaitFor { delegation.offset in delegation.donorHandled }
            delegation.releaseFreshResult.complete(Unit)

            awaitFor { delegation.metrics.processed.size == 2 }
            delegation.awaitCommit(freshOffset)
            assertEquals(listOf(delegation.offset), delegation.donorHandled.toList())
            assertEquals(
                mapOf(delegation.offset to false, freshOffset to true),
                delegation.metrics.processed.associate { it.record.offset() to it.processedRemotely }
            )
            assertEquals(
                listOf(delegation.offset to WarmupFallbackReason.REMOTE_DISCARD_REJECTED),
                delegation.metrics.warmupFallbacks.map { it.first.offset() to it.second }
            )
        }
    }

    private suspend fun withStaleResult(
        donorAgeLimit: Duration? = null,
        verify: suspend (StaleDelegation) -> Unit
    ) {
        val source = unique("discard-source")
        val warmup = unique("discard-warmup")
        val control = unique("discard-control")
        val namespace = unique("discard")
        val groupId = unique("discard-group")
        createTopics(source, warmup, control)

        val donorActive = CompletableDeferred<Unit>()
        val staleResult = CompletableDeferred<WarmupRemoteOutcome>()
        val freshResult = CompletableDeferred<WarmupRemoteOutcome>()
        val releaseStaleResult = CompletableDeferred<Unit>()
        val releaseFreshResult = CompletableDeferred<Unit>()
        val donorContext = faultInjectionContext(namespace, control, configure = {
            delegationTimeout = 25.seconds
        }, send = { message ->
            send(message).also {
                if (message is WarmupLivenessProbe && message.reply) donorActive.complete(Unit)
            }
        })
        val recipientContext = faultInjectionContext(namespace, control, configure = {
            warmupDuration = 40.seconds
            delegationTimeout = 25.seconds
        }, send = { message ->
            if (message is ControlRecordCompleted) {
                val outcome = message.completed.outcome
                if (outcome is WarmupRemoteOutcome.Discarded) {
                    staleResult.complete(outcome)
                    releaseStaleResult.await()
                } else {
                    freshResult.complete(outcome)
                    releaseFreshResult.await()
                }
            }
            send(message)
        })

        val metrics = RecordingWarmupMetrics<String, String>()
        val donorHandled = CopyOnWriteArrayList<Long>()
        val donor = coroutinesKafkaConsumer<String, String>(groupProperties(groupId, "z-donor")) {
            topics(source)
            commitIntervalMs = 50
            commitRecordsThreshold = 1
            workerConcurrency = 2
            this.metrics = metrics
            processingMode = if (donorAgeLimit == null) {
                ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            } else {
                ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST
            }
            freshnessMaxRecordAge = donorAgeLimit
            handle { donorHandled += it.offset() }
            trafficWarmup(donorContext, "records") {
                mapTopic(source, warmup)
                warmupConsumerProperties = warmupConsumerConnection()
                forwardingProducerProperties = forwardingConnection(StringSerializer::class.java)
            }
        }
        val recipient = coroutinesKafkaConsumer<String, String>(consumerProperties(groupId, StringDeserializer::class.java)) {
            topics(source)
            processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST
            freshnessMaxRecordAge = 10.seconds
            handle { check(it.value() == "fresh") { "An old warm-up record must be discarded" } }
            trafficWarmup(recipientContext, "records") {
                mapTopic(source, warmup)
                warmupConsumerProperties = warmupConsumerConnection()
                forwardingProducerProperties = forwardingConnection(StringSerializer::class.java)
            }
        }

        try {
            donor.start()
            withTimeout(15.seconds) { donor.awaitReady() }
            recipient.start()
            withTimeout(10.seconds) { donorActive.await() }

            val timestamp = System.currentTimeMillis() - 25.seconds.inWholeMilliseconds
            val sent = KafkaProducer(kafkaConnection(), StringSerializer(), StringSerializer()).use { producer ->
                producer.send(ProducerRecord(source, 0, timestamp, "key", "value")).get(10, TimeUnit.SECONDS)
            }
            assertEquals(
                WarmupRemoteOutcome.Discarded(WarmupDiscardReason.STALE_AGE),
                withTimeout(10.seconds) { staleResult.await() }
            )
            verify(
                StaleDelegation(
                    groupId, TopicPartition(source, sent.partition()), sent.offset(), timestamp,
                    donor, donorHandled, metrics, freshResult, releaseStaleResult, releaseFreshResult
                )
            )
        } finally {
            releaseStaleResult.complete(Unit)
            releaseFreshResult.complete(Unit)
            consumerContainer(recipient, donor).stop()
        }
    }

    // Range assignment gives the only partition to the first static member by instance ID.
    private fun groupProperties(groupId: String, instanceId: String) =
        consumerProperties(groupId, StringDeserializer::class.java) + mapOf(
            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG to instanceId,
            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG to RangeAssignor::class.java.name
        )

    private suspend fun StaleDelegation.awaitCommit(throughOffset: Long = offset) {
        Admin.create(kafkaConnection()).use { admin ->
            awaitFor {
                val offsets = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)
                offsets[partition]?.offset() == throughOffset + 1
            }
        }
    }

    private data class StaleDelegation(
        val groupId: String,
        val partition: TopicPartition,
        val offset: Long,
        val timestamp: Long,
        val donor: CoroutinesKafkaConsumer<String, String>,
        val donorHandled: List<Long>,
        val metrics: RecordingWarmupMetrics<String, String>,
        val freshResult: CompletableDeferred<WarmupRemoteOutcome>,
        val releaseStaleResult: CompletableDeferred<Unit>,
        val releaseFreshResult: CompletableDeferred<Unit>
    )
}
