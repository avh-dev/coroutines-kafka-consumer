package avh.ckc.core.warmup

import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class LogicalWarmupDonorSessionTest {
    @Test
    fun `logical admission atomically enforces session count and normalized aggregate budget`() {
        val store = LogicalWarmupDonorSessionStore(maxSessions = 2, maxAggregateBudgetPerSecond = 150)

        assertEquals(DonorSessionAdmission.ACCEPTED, store.tryAdmitPreparation(WarmupSessionId("one"), 100))
        assertEquals(DonorSessionAdmission.BUDGET_LIMIT, store.tryAdmitPreparation(WarmupSessionId("two"), 60))
        assertEquals(1, store.admittedSessionCount())
        assertEquals(100, store.admittedBudgetPerSecond())

        assertTrue(store.cancelPreparingAdmission(WarmupSessionId("one")))
        assertEquals(DonorSessionAdmission.ACCEPTED, store.tryAdmitPreparation(WarmupSessionId("two"), 60))
        assertEquals(DonorSessionAdmission.ACCEPTED, store.tryAdmitPreparation(WarmupSessionId("three"), 90))
        assertEquals(DonorSessionAdmission.SESSION_LIMIT, store.tryAdmitPreparation(WarmupSessionId("four"), 1))
        assertEquals(150, store.admittedBudgetPerSecond())
    }

    @Test
    fun `participant mappings consume independent quotas with different intervals`() = runBlocking {
        val sessionId = WarmupSessionId("independent-mappings")
        val dataPlane = WarmupParticipantDataPlane<String, String>()
        val slow = mapping("slow", WarmupMappingQuota(1, quotaIntervalMillis = 2_000))
        val busy = mapping("busy", WarmupMappingQuota(2, quotaIntervalMillis = 500))
        val sessions = mapOf(
            slow to session(sessionId, slow),
            busy to session(sessionId, busy)
        )
        val lane = WarmupParticipantDonorLane(sessionId, dataPlane, sessions)

        assertEquals(5, lane.admissionBudgetPerSecond)
        assertTrue(lane.activate())
        assertNotNull(lane.tryBeginDelegation(record("slow", 0)))
        assertNull(lane.tryBeginDelegation(record("slow", 1)))
        assertNotNull(lane.tryBeginDelegation(record("busy", 0)))
        assertNotNull(lane.tryBeginDelegation(record("busy", 1)))
        assertNull(lane.tryBeginDelegation(record("busy", 2)))
    }

    @Test
    fun `admission budget uses the maximum quota anywhere in the profile`() {
        val quota = WarmupMappingQuota(
            WarmupQuotaProfileParser.parse("5 -> (10s) -> 100 -> (10s) -> 20"),
            quotaIntervalMillis = 2_000
        )

        assertEquals(50, quota.admissionBudgetPerSecond())
    }

    private fun mapping(topic: String, quota: WarmupMappingQuota) = WarmupTopicMapping(
        WarmupParticipantId("participant"),
        topic,
        "warmup-$topic",
        "fingerprint-$topic",
        quota
    )

    private fun session(
        sessionId: WarmupSessionId,
        mapping: WarmupTopicMapping
    ) = WarmupDonorSession(
        sessionId = sessionId,
        quotaProfile = mapping.quota.profile,
        quotaInterval = mapping.quota.quotaIntervalMillis.milliseconds,
        delegationTimeout = 1.seconds,
        destinationResolver = WarmupDestinationResolver { WarmupDestination(mapping.warmupTopic) },
        sender = object : WarmupRecordSender<String, String> {
            override suspend fun send(
                record: ConsumerRecord<String, String>,
                destination: WarmupDestination,
                sessionId: WarmupSessionId
            ) = WarmupSendAcknowledgement(TopicPartition(destination.topic, 0), record.offset())
        }
    )

    private fun record(topic: String, offset: Long) = ConsumerRecord(topic, 0, offset, "key", "value")
}
