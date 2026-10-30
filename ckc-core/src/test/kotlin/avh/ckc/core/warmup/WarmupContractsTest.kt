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

        val traffic = WarmupTopicCapability(
            sourceTopic = "orders",
            compatible = true,
            ownedPartitions = setOf(1, 3),
            observedMessages = 25
        )
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
    fun `topic outcome separates traffic drain and delivery audit`() {
        val noTraffic = topicOutcome(
            forwardedMessages = 0,
            successfullyProcessedMessages = 0,
            drainStatus = WarmupDrainStatus.NO_RECORDS
        )
        assertEquals(WarmupTrafficStatus.NO_REAL_TRAFFIC, noTraffic.trafficStatus)
        assertTrue(noTraffic.drainCompleted)
        assertFalse(noTraffic.deliveryVerified)

        val observedBusinessProcessingWithAuditViolation = topicOutcome(
            forwardedMessages = 10,
            successfullyProcessedMessages = 11,
            drainStatus = WarmupDrainStatus.COMPLETED,
            deliveryAuditStatus = WarmupDeliveryAuditStatus.VIOLATIONS_DETECTED
        )
        assertEquals(
            WarmupTrafficStatus.BUSINESS_PROCESSING_OBSERVED,
            observedBusinessProcessingWithAuditViolation.trafficStatus
        )
        assertTrue(observedBusinessProcessingWithAuditViolation.drainCompleted)
        assertFalse(observedBusinessProcessingWithAuditViolation.deliveryVerified)

        val verified = topicOutcome(
            forwardedMessages = 10,
            successfullyProcessedMessages = 10,
            drainStatus = WarmupDrainStatus.COMPLETED,
            deliveryAuditStatus = WarmupDeliveryAuditStatus.VERIFIED
        )
        assertTrue(verified.deliveryVerified)

        assertEquals(
            WarmupTrafficStatus.FORWARDED_WITHOUT_SUCCESSFUL_PROCESSING,
            topicOutcome(
                forwardedMessages = 10,
                successfullyProcessedMessages = 0,
                drainStatus = WarmupDrainStatus.COMPLETED
            ).trafficStatus
        )
    }

    @Test
    fun `no records drain cannot contain successful business processing`() {
        assertFailsWith<IllegalArgumentException> {
            topicOutcome(
                forwardedMessages = 0,
                successfullyProcessedMessages = 1,
                drainStatus = WarmupDrainStatus.NO_RECORDS
            )
        }
        assertFailsWith<IllegalArgumentException> {
            topicOutcome(
                ownedSourcePartitions = emptySet(),
                forwardedMessages = 1,
                drainStatus = WarmupDrainStatus.NOT_STARTED
            )
        }
    }

    @Test
    fun `contract collections are immutable snapshots at every nesting level`() {
        val mappings = mutableListOf(WarmupTopicMapping("orders", "warmup-orders"))
        val configuration = WarmupParticipantConfiguration(WarmupConsumerId("orders"), mappings)
        mappings += WarmupTopicMapping("batches", "warmup-batches")
        assertEquals(listOf(WarmupTopicMapping("orders", "warmup-orders")), configuration.topicMappings)

        val partitionAssignments = mutableListOf(
            WarmupPartitionAssignment(WarmupConsumerId("orders"), "orders", "warmup-orders", 0)
        )
        val session = WarmupSessionAssignment(
            WarmupSessionId("session"),
            WarmupInstanceId("donor"),
            WarmupInstanceId("recipient"),
            partitionAssignments
        )
        partitionAssignments += partitionAssignments.single().copy(warmupPartition = 1)
        assertEquals(1, session.partitions.size)

        val ownedPartitions = mutableSetOf(0)
        val topicCapability = WarmupTopicCapability("orders", true, ownedPartitions, 1)
        val topics = mutableListOf(topicCapability)
        val participantCapability = WarmupParticipantCapability(WarmupConsumerId("orders"), topics)
        val participants = mutableListOf(participantCapability)
        val donorCapability = WarmupDonorCapability(WarmupInstanceId("donor"), true, participants)
        ownedPartitions += 1
        topics += WarmupTopicCapability("batches", true)
        participants += WarmupParticipantCapability(
            WarmupConsumerId("batches"),
            listOf(WarmupTopicCapability("batches", true))
        )
        assertEquals(setOf(0), topicCapability.ownedPartitions)
        assertEquals(1, participantCapability.topics.size)
        assertEquals(1, donorCapability.participants.size)

        val outcomePartitions = mutableSetOf(0)
        val outcome = topicOutcome(ownedSourcePartitions = outcomePartitions)
        outcomePartitions += 1
        assertEquals(setOf(0), outcome.ownedSourcePartitions)

        assertFailsWith<UnsupportedOperationException> {
            (configuration.topicMappings as MutableList<WarmupTopicMapping>).clear()
        }
        assertFailsWith<UnsupportedOperationException> {
            (topicCapability.ownedPartitions as MutableSet<Int>).clear()
        }
    }

    private fun topicOutcome(
        ownedSourcePartitions: Collection<Int> = setOf(0),
        forwardedMessages: Long = 0,
        successfullyProcessedMessages: Long = 0,
        drainStatus: WarmupDrainStatus = WarmupDrainStatus.NOT_STARTED,
        deliveryAuditStatus: WarmupDeliveryAuditStatus = WarmupDeliveryAuditStatus.NOT_ANALYZED
    ): WarmupTopicOutcome =
        WarmupTopicOutcome(
            consumerId = WarmupConsumerId("orders"),
            sourceTopic = "orders",
            warmupTopic = "ckc.traffic-warmup.v1.orders",
            compatible = true,
            ownedSourcePartitions = ownedSourcePartitions,
            observedMessages = 100,
            forwardedMessages = forwardedMessages,
            successfullyProcessedMessages = successfullyProcessedMessages,
            forwardedMessagesPerSecond = if (forwardedMessages == 0L) 0.0 else 2.0,
            drainStatus = drainStatus,
            deliveryAuditStatus = deliveryAuditStatus
        )
}
