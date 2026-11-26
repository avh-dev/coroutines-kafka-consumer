package avh.ckc.warmup

import avh.ckc.warmup.dataplane.WarmupDestination
import avh.ckc.warmup.dataplane.WarmupRecordSender
import avh.ckc.warmup.dataplane.WarmupSendAcknowledgement
import avh.ckc.warmup.donor.LogicalWarmupDonorSession
import avh.ckc.warmup.donor.WarmupDestinationResolver
import avh.ckc.warmup.donor.WarmupDonorEligibility
import avh.ckc.warmup.donor.WarmupDonorSession
import avh.ckc.warmup.donor.WarmupDonorSessionManager
import avh.ckc.warmup.protocol.DonorAccepted
import avh.ckc.warmup.protocol.RecipientReady
import avh.ckc.warmup.protocol.SessionAccepted
import avh.ckc.warmup.protocol.SessionEnded
import avh.ckc.warmup.protocol.WarmupCompletionRequested
import avh.ckc.warmup.protocol.WarmupControlMessage
import avh.ckc.warmup.protocol.WarmupControlNamespace
import avh.ckc.warmup.protocol.WarmupDonorMapping
import avh.ckc.warmup.protocol.WarmupInstanceId
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupPartitionAssignment
import avh.ckc.warmup.protocol.WarmupQuotaProfile
import avh.ckc.warmup.protocol.WarmupSessionEndState
import avh.ckc.warmup.protocol.WarmupSessionId
import avh.ckc.warmup.protocol.WarmupSourcePartition
import avh.ckc.warmup.protocol.WarmupTopicMapping
import avh.ckc.warmup.protocol.normalizedAdmissionBudget
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CoroutineScope
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertTrue

/** Protocol messages and mapping sessions used to set up donor manager tests. */
internal abstract class WarmupDonorSessionFixture {
    protected val namespace = WarmupControlNamespace("global5-shared-partition-v6-test")

    protected val recipient = WarmupInstanceId("recipient")

    protected val donor = WarmupInstanceId("donor")

    protected val mapping = WarmupTopicMapping(
        WarmupParticipantId("orders"), "orders", "warmup-orders", "mapping-fingerprint"
    )

    protected val ownership = setOf(WarmupSourcePartition(0, 3))

    protected fun activation(
        scope: CoroutineScope,
        plane: WarmupDonorDataPlaneFixture<String, String>,
        prepare: suspend () -> WarmupDonorSession<String, String>
    ) = WarmupDonorSessionManager(
        localInstanceId = donor,
        namespace = namespace,
        supportedMappings = listOf(mapping),
        dataPlane = plane.store,
        eligibility = WarmupDonorEligibility { ownership },
        preparation = plane.preparation { prepare() },
        scope = scope,
        onPreparationFailed = {},
        nowEpochMillis = { 1_000 }
    )

    protected fun acceptAndStart(
        activation: WarmupDonorSessionManager<LogicalWarmupDonorSession>,
        acceptance: SessionAccepted
    ): WarmupControlMessage = activation.accept(acceptance).also { response ->
        if (response is DonorAccepted) assertTrue(activation.startPreparation(acceptance.sessionId))
    }

    protected fun acceptance() = SessionAccepted(
        namespace, WarmupSessionId("session"), recipient, donor,
        listOf(WarmupDonorMapping(mapping, ownership)),
        normalizedAdmissionBudget(listOf(mapping)), "fingerprint", 10_000
    )

    protected fun ready() = RecipientReady(
        namespace, WarmupSessionId("session"), recipient, donor,
        listOf(WarmupPartitionAssignment(mapping)), "fingerprint", 10_000
    )

    protected fun completion(acceptance: SessionAccepted) = WarmupCompletionRequested(
        namespace,
        acceptance.sessionId,
        recipient,
        donor,
        acceptance.configurationFingerprint,
        acceptance.expiresAtEpochMillis
    )

    protected fun fallbackEnded() = SessionEnded(
        namespace, WarmupSessionId("session"), recipient,
        WarmupSessionEndState.COLD_FALLBACK, "preparation_cancelled", "fingerprint", 10_000
    )

    protected fun session(
        sessionId: WarmupSessionId,
        closed: AtomicInteger = AtomicInteger(),
        onClose: () -> Unit = { closed.incrementAndGet() }
    ): WarmupDonorSession<String, String> {
        val sender = object : WarmupRecordSender<String, String>, AutoCloseable {
            override suspend fun send(
                record: ConsumerRecord<String, String>,
                destination: WarmupDestination,
                sessionId: WarmupSessionId
            ) = WarmupSendAcknowledgement(TopicPartition(destination.topic, destination.partition), 0)

            override fun close() {
                onClose()
            }
        }
        return WarmupDonorSession(
            sessionId = sessionId,
            quotaProfile = WarmupQuotaProfile.constant(10),
            quotaInterval = 1.seconds,
            delegationTimeout = 1.seconds,
            destinationResolver = WarmupDestinationResolver { WarmupDestination("warmup-orders") },
            sender = sender
        )
    }
}
