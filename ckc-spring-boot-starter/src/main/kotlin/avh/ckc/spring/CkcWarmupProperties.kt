package avh.ckc.spring

import java.time.Duration

/** Named contexts for opt-in traffic warm-up. Unset tuning values use the warm-up DSL defaults. */
data class CkcWarmupProperties(
    var enabled: Boolean = false,
    var contexts: Map<String, Context> = emptyMap()
) {
    data class Context(
        var instanceId: String? = null,
        var namespace: String = "",
        var controlTopic: String = "",
        var controlCluster: String = "",
        var warmupDuration: Duration? = null,
        var offerWindow: Duration? = null,
        var participantStartTimeout: Duration? = null,
        var preparationTimeout: Duration? = null,
        var completionTimeout: Duration? = null,
        var delegationTimeout: Duration? = null,
        var donorLivenessTimeout: Duration? = null,
        var maxDonorSessions: Int? = null,
        var maxAggregateQuotaPerSecond: Int? = null,
        var maxInflightDelegations: Int? = null,
        var serviceThreadLimit: Int? = null,
        var serviceThreadIdleTimeout: Duration? = null,
        var brokerLifecycle: BrokerLifecycle? = null
    )

    data class BrokerLifecycle(
        var topic: String = "",
        var cluster: String? = null,
        var operationTimeout: Duration? = null
    )

    data class Participant(
        var context: String = "",
        var destinationCluster: String = "",
        var keySerializer: String = "",
        var valueSerializer: String = "",
        var producerProperties: Map<String, String> = emptyMap(),
        var consumerProperties: Map<String, String> = emptyMap(),
        var mappings: List<Mapping> = emptyList()
    )

    data class Mapping(
        var sourceTopic: String = "",
        var warmupTopic: String = "",
        var quotaProfile: String? = null,
        var quotaInterval: Duration? = null
    )
}
