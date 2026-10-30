package avh.ckc.core.warmup

import avh.ckc.core.ProcessingMode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class WarmupContractsTest {
    @Test
    fun `identifiers reject blank long and control character values`() {
        assertFailsWith<IllegalArgumentException> { WarmupInstanceId(" ") }
        assertFailsWith<IllegalArgumentException> { WarmupConsumerId("a".repeat(256)) }
        assertFailsWith<IllegalArgumentException> { WarmupSessionId("session\n1") }
    }

    @Test
    fun `participant configuration accepts only unordered at least once with unique source topics`() {
        val mapping = WarmupTopicMapping("orders", "ckc.traffic-warmup.v1.orders")

        assertFailsWith<IllegalArgumentException> {
            WarmupParticipantConfiguration(
                consumerId = WarmupConsumerId("orders"),
                topicMappings = listOf(mapping),
                processingMode = ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING
            )
        }
        assertFailsWith<IllegalArgumentException> {
            WarmupParticipantConfiguration(
                consumerId = WarmupConsumerId("orders"),
                topicMappings = listOf(mapping, mapping)
            )
        }

        val configuration = WarmupParticipantConfiguration(
            consumerId = WarmupConsumerId("orders"),
            topicMappings = listOf(mapping)
        )
        assertEquals(ProcessingMode.AT_LEAST_ONCE_NO_ORDERING, configuration.processingMode)
    }

    @Test
    fun `forwarding profile validates fractions and durations`() {
        assertFailsWith<IllegalArgumentException> {
            WarmupForwardingProfile(initialFraction = 0.2, targetFraction = 0.1)
        }
        assertFailsWith<IllegalArgumentException> {
            WarmupForwardingProfile(targetFraction = Double.NaN)
        }
        assertFailsWith<IllegalArgumentException> {
            WarmupForwardingProfile(rampUpDuration = (-1).seconds)
        }

        val profile = WarmupForwardingProfile(
            initialFraction = 0.01,
            targetFraction = 0.1,
            rampUpDuration = 30.seconds,
            holdDuration = 10.seconds
        )
        assertEquals(0.1, profile.targetFraction)
    }

    @Test
    fun `session assignment has one destination for every consumer source pair`() {
        val assignment = WarmupPartitionAssignment(
            consumerId = WarmupConsumerId("orders"),
            sourceTopic = "orders",
            warmupTopic = "ckc.traffic-warmup.v1.orders",
            warmupPartition = 2
        )

        assertFailsWith<IllegalArgumentException> {
            WarmupSessionAssignment(
                sessionId = WarmupSessionId("session-1"),
                donorInstanceId = WarmupInstanceId("pod-a"),
                recipientInstanceId = WarmupInstanceId("pod-a"),
                partitions = listOf(assignment)
            )
        }
        assertFailsWith<IllegalArgumentException> {
            WarmupSessionAssignment(
                sessionId = WarmupSessionId("session-1"),
                donorInstanceId = WarmupInstanceId("pod-a"),
                recipientInstanceId = WarmupInstanceId("pod-b"),
                partitions = listOf(assignment, assignment.copy(warmupPartition = 3))
            )
        }

        val session = WarmupSessionAssignment(
            sessionId = WarmupSessionId("session-1"),
            donorInstanceId = WarmupInstanceId("pod-a"),
            recipientInstanceId = WarmupInstanceId("pod-b"),
            partitions = listOf(assignment)
        )
        assertEquals(listOf(assignment), session.assignmentsFor(WarmupConsumerId("orders")))
    }

    @Test
    fun `session assignment does not share a warm-up partition across source topics`() {
        val destination = "ckc.traffic-warmup.v1.shared"
        assertFailsWith<IllegalArgumentException> {
            WarmupSessionAssignment(
                sessionId = WarmupSessionId("session-1"),
                donorInstanceId = WarmupInstanceId("pod-a"),
                recipientInstanceId = WarmupInstanceId("pod-b"),
                partitions = listOf(
                    WarmupPartitionAssignment(WarmupConsumerId("orders"), "orders", destination, 0),
                    WarmupPartitionAssignment(WarmupConsumerId("batches"), "batches", destination, 0)
                )
            )
        }
    }

    @Test
    fun `donor capability distinguishes compatibility ownership and observed traffic`() {
        val noTraffic = WarmupTopicCapability(
            sourceTopic = "orders",
            compatible = true,
            ownedPartitions = setOf(1, 3)
        )
        assertTrue(noTraffic.ownsCandidatePartitions)
        assertFalse(noTraffic.hasObservedTraffic)

        val traffic = noTraffic.copy(observedMessages = 25)
        assertTrue(traffic.hasObservedTraffic)

        val incompatibleButActive = WarmupTopicCapability(
            sourceTopic = "orders",
            compatible = false,
            ownedPartitions = setOf(1),
            observedMessages = 10
        )
        assertTrue(incompatibleButActive.ownsCandidatePartitions)
        assertTrue(incompatibleButActive.hasObservedTraffic)
    }

    @Test
    fun `topic result never treats time or configuration alone as real traffic warm-up`() {
        val base = WarmupTopicOutcome(
            consumerId = WarmupConsumerId("orders"),
            sourceTopic = "orders",
            warmupTopic = "ckc.traffic-warmup.v1.orders",
            compatible = true,
            ownedSourcePartitions = setOf(0),
            observedMessages = 100,
            forwardedMessages = 0,
            processedMessages = 0,
            forwardedMessagesPerSecond = 0.0,
            drainStatus = WarmupDrainStatus.NO_RECORDS
        )

        assertEquals(WarmupTopicResult.NO_REAL_TRAFFIC, base.result)
        assertEquals(
            WarmupTopicResult.NOT_CURRENTLY_OWNED,
            base.copy(ownedSourcePartitions = emptySet()).result
        )
        assertEquals(
            WarmupTopicResult.WARMED_WITH_REAL_TRAFFIC,
            base.copy(
                forwardedMessages = 10,
                processedMessages = 10,
                forwardedMessagesPerSecond = 2.0,
                drainStatus = WarmupDrainStatus.COMPLETED
            ).result
        )
        assertEquals(
            WarmupTopicResult.INCOMPLETE,
            base.copy(
                forwardedMessages = 10,
                processedMessages = 9,
                forwardedMessagesPerSecond = 2.0,
                drainStatus = WarmupDrainStatus.COMPLETED
            ).result
        )
        assertEquals(
            WarmupTopicResult.FAILED,
            base.copy(
                forwardedMessages = 10,
                processedMessages = 9,
                forwardedMessagesPerSecond = 2.0,
                drainStatus = WarmupDrainStatus.FAILED
            ).result
        )
    }
}
