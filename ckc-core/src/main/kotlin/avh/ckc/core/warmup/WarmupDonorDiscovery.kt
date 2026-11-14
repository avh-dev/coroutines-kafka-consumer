package avh.ckc.core.warmup

import avh.ckc.core.CoroutinesKafkaConsumer

/**
 * Supplies the donor's current main-consumer readiness and source ownership. Returning null means
 * the participant is not currently eligible; callers must not infer readiness from stale offers.
 */
internal fun interface WarmupDonorEligibility {
    fun eligibleOwnership(mapping: WarmupTopicMapping): Set<WarmupSourcePartition>?
}

/** Binds discovery eligibility to actual ready main consumers and their live ownership generations. */
internal class MainConsumerWarmupDonorEligibility(
    participants: Map<WarmupParticipantId, CoroutinesKafkaConsumer<*, *>>
) : WarmupDonorEligibility {
    private val participants = participants.toMap()

    override fun eligibleOwnership(mapping: WarmupTopicMapping): Set<WarmupSourcePartition>? =
        participants[mapping.participantId]?.currentWarmupSourceOwnership(mapping.sourceTopic)
}

/** Produces stateless offers. Capacity is deliberately checked only by final admission. */
internal class StatelessWarmupDonorDiscovery(
    private val localInstanceId: WarmupInstanceId,
    private val namespace: WarmupControlNamespace,
    supportedMappings: Collection<WarmupTopicMapping>,
    private val eligibility: WarmupDonorEligibility
) {
    private val supportedMappings = supportedMappings.associateBy(::mappingIdentity)

    fun offer(request: WarmupRequested, nowEpochMillis: Long): DonorOffered? {
        if (request.protocolVersion != WARMUP_CONTROL_PROTOCOL_VERSION) return null
        if (request.namespace != namespace || request.senderInstanceId == localInstanceId) return null
        if (nowEpochMillis > request.offerDeadlineEpochMillis || nowEpochMillis > request.expiresAtEpochMillis) return null
        val eligible = request.mappings.mapNotNull { requested ->
            val supported = supportedMappings[mappingIdentity(requested)] ?: return@mapNotNull null
            if (supported != requested) return@mapNotNull null
            val ownership = eligibility.eligibleOwnership(supported)?.takeIf(Set<WarmupSourcePartition>::isNotEmpty)
                ?: return@mapNotNull null
            WarmupDonorMapping(supported, ownership)
        }
        if (eligible.isEmpty()) return null
        return DonorOffered(
            namespace = namespace,
            sessionId = request.sessionId,
            senderInstanceId = localInstanceId,
            targetInstanceId = request.senderInstanceId,
            mappings = eligible,
            configurationFingerprint = request.configurationFingerprint,
            expiresAtEpochMillis = request.expiresAtEpochMillis
        )
    }
}

internal fun mappingIdentity(mapping: WarmupTopicMapping): Pair<WarmupParticipantId, String> =
    mapping.participantId to mapping.sourceTopic
