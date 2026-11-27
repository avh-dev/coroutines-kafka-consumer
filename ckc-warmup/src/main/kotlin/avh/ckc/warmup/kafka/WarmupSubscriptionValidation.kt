package avh.ckc.warmup.kafka

import avh.ckc.warmup.runtime.RegisteredTrafficWarmupParticipant
import java.time.Duration as JavaDuration
import kotlin.time.Duration
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.clients.admin.Admin
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/** Checks possible topic intersections before starting either side of the warm-up protocol. */
internal class WarmupSubscriptionValidation(
    private val timeout: Duration,
    private val dispatcher: CoroutineDispatcher,
    private val adminFactory: (Map<String, Any?>) -> Admin = Admin::create,
    private val log: Logger = LoggerFactory.getLogger(WarmupSubscriptionValidation::class.java)
) {
    suspend fun failureReason(participants: List<RegisteredTrafficWarmupParticipant>): String? {
        val subscribers = participants.flatMap { participant ->
            participant.mainTopics.map { topic -> topic to participant }
        }.groupBy({ it.first }, { it.second })

        val candidates = participants.flatMap { sender ->
            sender.mappings.flatMap { mapping ->
                subscribers[mapping.warmupTopic].orEmpty().map { subscriber ->
                    Candidate(sender, mapping.sourceTopic, mapping.warmupTopic, subscriber)
                }
            }
        }
        if (candidates.isEmpty()) return null

        var candidate = candidates.first()
        return try {
            val conflict = withTimeoutOrNull(timeout) {
                runInterruptible(dispatcher) {
                    val clusterIds = mutableMapOf<Map<String, Any?>, String>()
                    fun clusterId(properties: Map<String, Any?>): String =
                        clusterIds.getOrPut(properties) {
                            val admin = adminFactory(properties)
                            try {
                                val id = admin.describeCluster().clusterId().get()
                                check(!id.isNullOrBlank()) { "Kafka returned no cluster ID" }
                                id
                            } finally {
                                admin.close(JavaDuration.ZERO)
                            }
                        }

                    candidates.any {
                        candidate = it
                        clusterId(it.sender.forwardingProducerProperties) == clusterId(it.subscriber.mainProperties)
                    }
                }
            }
            when (conflict) {
                true -> {
                    log.warn("Warm-up route overlaps a main subscription on the same Kafka cluster: {}", candidate)
                    "warmup_subscription_overlap"
                }
                false -> null
                null -> unavailableClusterIdentity(candidate, "timeout")
            }
        } catch (error: CancellationException) {
            throw error
        } catch (error: Exception) {
            unavailableClusterIdentity(candidate, unwrapKafkaMetadataFailure(error).javaClass.simpleName)
        }
    }

    private fun unavailableClusterIdentity(candidate: Candidate, causeType: String): String {
        val reason = "warmup_cluster_identity_unavailable"
        log.warn("Warm-up route validation fallback reason={} route={} cause_type={}", reason, candidate, causeType)
        return reason
    }

    private class Candidate(
        val sender: RegisteredTrafficWarmupParticipant,
        val sourceTopic: String,
        val warmupTopic: String,
        val subscriber: RegisteredTrafficWarmupParticipant
    ) {
        override fun toString(): String =
            "participant '${sender.participantId.value}', mapping '$sourceTopic' -> '$warmupTopic', " +
                "main subscription '${subscriber.participantId.value}' to '$warmupTopic'"
    }
}
