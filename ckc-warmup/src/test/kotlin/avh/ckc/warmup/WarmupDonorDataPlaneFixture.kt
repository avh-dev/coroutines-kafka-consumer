package avh.ckc.warmup

import avh.ckc.warmup.dataplane.WarmupParticipantDataPlane
import avh.ckc.warmup.dataplane.WarmupParticipantDonorLane
import avh.ckc.warmup.donor.DonorSessionAdmission
import avh.ckc.warmup.donor.LogicalWarmupDonorSession
import avh.ckc.warmup.donor.LogicalWarmupDonorSessionStore
import avh.ckc.warmup.donor.WarmupDonorSession
import avh.ckc.warmup.donor.WarmupDonorSessionPreparation
import avh.ckc.warmup.protocol.SessionAccepted
import avh.ckc.warmup.protocol.WarmupSessionId
import avh.ckc.warmup.protocol.WarmupTopicMapping

/** Connects a typed test participant to the same logical donor store used by the DSL. */
internal class WarmupDonorDataPlaneFixture<K, V>(maxSessions: Int, maxAggregateBudgetPerSecond: Int) {
    val store = LogicalWarmupDonorSessionStore(maxSessions, maxAggregateBudgetPerSecond)
    val records = WarmupParticipantDataPlane<K, V>()

    fun preparation(
        prepare: suspend (SessionAccepted) -> WarmupDonorSession<K, V>
    ) = WarmupDonorSessionPreparation { acceptance ->
        val mapping = acceptance.mappings.single().mapping
        logicalSession(mapping, prepare(acceptance))
    }

    fun admit(mapping: WarmupTopicMapping, session: WarmupDonorSession<K, V>): DonorSessionAdmission {
        val admission = store.tryAdmitPreparation(session.sessionId, session.admissionBudgetPerSecond)
        if (admission == DonorSessionAdmission.ACCEPTED) {
            check(store.installAdmittedSession(logicalSession(mapping, session)))
        }
        return admission
    }

    fun removeDrainedSession(sessionId: WarmupSessionId): Boolean {
        val session = store.detachDrainedSession(sessionId) ?: return false
        session.close()
        return true
    }

    private fun logicalSession(
        mapping: WarmupTopicMapping,
        session: WarmupDonorSession<K, V>
    ): LogicalWarmupDonorSession {
        val lane = WarmupParticipantDonorLane(session.sessionId, records, mapOf(mapping to session))
        return LogicalWarmupDonorSession(session.sessionId, session.admissionBudgetPerSecond, listOf(lane))
    }
}
