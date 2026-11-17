package avh.ckc.core.warmup

import avh.ckc.core.metrics.RecordDropReason
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class WarmupControlCodecTest {
    private val namespace = WarmupControlNamespace("global5-shared-partition-v6-test")
    private val recipient = WarmupInstanceId("recipient")
    private val donor = WarmupInstanceId("donor")
    private val session = WarmupSessionId("session")
    private val mapping = WarmupTopicMapping(
        WarmupParticipantId("orders"),
        "orders",
        "warmup-orders",
        "mapping-fingerprint",
        WarmupMappingQuota(10, 1, 1_000, 30_000, QuotaRampCurve.LINEAR)
    )
    private val donorMapping = WarmupDonorMapping(mapping, setOf(WarmupSourcePartition(0, 7)))
    private val fingerprint = "context-fingerprint"

    @Test
    fun `every v6 control message round trips with namespace and exact immutable fields`() {
        val completed = WarmupRecordCompleted(
            WarmupRecordId(session, TopicPartition("warmup-orders", 0), 91),
            WarmupRemoteOutcome.Discarded(RecordDropReason.STALE_AGE)
        )
        val messages = listOf(
            WarmupRequested(namespace, session, recipient, listOf(mapping), 2_000, 10, fingerprint, 10_000),
            DonorOffered(namespace, session, donor, recipient, listOf(donorMapping), fingerprint, 10_000),
            SessionAccepted(namespace, session, recipient, donor, listOf(donorMapping), 10, fingerprint, 10_000),
            DonorAccepted(namespace, session, donor, recipient, listOf(donorMapping), 10, fingerprint, 10_000),
            SessionRejected(namespace, session, donor, recipient, "capacity_limit", fingerprint, 10_000),
            RecipientReady(
                namespace, session, recipient, donor, listOf(WarmupPartitionAssignment(mapping)), fingerprint, 10_000
            ),
            ControlRecordCompleted(namespace, session, recipient, donor, completed, fingerprint, 10_000),
            WarmupCompletionRequested(namespace, session, recipient, donor, fingerprint, 10_000),
            DonorClosed(namespace, session, donor, recipient, fingerprint, 10_000),
            SessionEnded(namespace, session, recipient, WarmupSessionEndState.COLD_FALLBACK, "metadata_failed", fingerprint, 10_000)
        )

        messages.forEach { message -> assertEquals(message, WarmupControlCodec.decode(WarmupControlCodec.encode(message))) }
    }

    @Test
    fun `v5 envelope is rejected before dispatch`() {
        val encoded = WarmupControlCodec.encode(
            WarmupRequested(namespace, session, recipient, listOf(mapping), 2_000, 10, fingerprint, 10_000)
        )
        encoded[5] = 5

        assertThrows(IllegalArgumentException::class.java) { WarmupControlCodec.decode(encoded) }
    }
}
