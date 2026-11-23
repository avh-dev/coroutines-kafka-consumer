package avh.ckc.warmup

import avh.ckc.core.KafkaRecordHandler
import avh.ckc.core.createTestConsumer
import avh.ckc.core.stringSerdeProperties
import avh.ckc.core.typedTestRecord
import avh.ckc.warmup.protocol.WarmupControlNamespace
import avh.ckc.warmup.protocol.WarmupInstanceId
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupRequested
import avh.ckc.warmup.protocol.WarmupSessionId
import avh.ckc.warmup.protocol.WarmupSourcePartition
import avh.ckc.warmup.protocol.WarmupTopicMapping
import avh.ckc.warmup.protocol.normalizedAdmissionBudget
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class StatelessWarmupDonorDiscoveryTest {
    private val namespace = WarmupControlNamespace("global5-shared-partition-v6-test")
    private val recipient = WarmupInstanceId("recipient")
    private val donor = WarmupInstanceId("donor")
    private val mapping = WarmupTopicMapping(
        WarmupParticipantId("orders"), "orders", "warmup-orders", "mapping-fingerprint"
    )

    @Test
    fun `offer reports only current ready ownership and does not require capacity admission`() {
        var ownership: Set<WarmupSourcePartition>? = setOf(WarmupSourcePartition(0, 9))
        val discovery = StatelessWarmupDonorDiscovery(
            donor, namespace, listOf(mapping), WarmupDonorEligibility { ownership }
        )
        val request = WarmupRequested(
            namespace, WarmupSessionId("session"), recipient, listOf(mapping),
            2_000, normalizedAdmissionBudget(listOf(mapping)), "fingerprint", 10_000
        )

        val first = discovery.offer(request, 1_000)
        val repeated = discovery.offer(request, 1_000)
        assertEquals(first, repeated)
        assertEquals(ownership, first?.mappings?.single()?.sourcePartitions)

        ownership = null
        assertNull(discovery.offer(request, 1_000))
    }

    @Test
    fun `main consumer eligibility requires readiness and exposes current ownership generation`() = runBlocking {
        val consumer = createTestConsumer(
            records = listOf(typedTestRecord(offset = 0)),
            consumerProperties = stringSerdeProperties(),
            handler = KafkaRecordHandler { }
        )
        val participantId = WarmupParticipantId("orders")
        val eligibility = MainConsumerWarmupDonorEligibility(mapOf(participantId to consumer))
        val sourceMapping = WarmupTopicMapping(
            participantId, "topic-a", "warmup-orders", "mapping-fingerprint"
        )

        assertNull(eligibility.eligibleOwnership(sourceMapping))
        consumer.start()
        consumer.awaitReady()

        val ownership = eligibility.eligibleOwnership(sourceMapping)
        assertEquals(setOf(0), ownership?.map { it.partition }?.toSet())
        assertEquals(1, ownership?.single()?.ownershipGeneration)

        consumer.stop()
        assertNull(eligibility.eligibleOwnership(sourceMapping))
    }
}
