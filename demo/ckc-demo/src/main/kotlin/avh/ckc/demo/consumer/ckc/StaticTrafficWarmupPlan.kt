package avh.ckc.demo.consumer.ckc

import avh.ckc.core.ProcessingMode
import avh.ckc.core.warmup.WarmupConsumerId
import avh.ckc.core.warmup.WarmupForwardingConfiguration
import avh.ckc.core.warmup.WarmupForwardingObserver
import avh.ckc.core.warmup.WarmupForwardingProfile
import avh.ckc.core.warmup.WarmupPartitionAssignment
import avh.ckc.core.warmup.WarmupSessionId
import avh.ckc.demo.DemoTopics
import avh.ckc.demo.audit.auditTopicIdOrNull
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.config.kafkaConnectionProperties
import avh.ckc.demo.logWarmupForwarded
import avh.ckc.demo.serialization.BatchLifecycleEventSerializer
import avh.ckc.demo.serialization.CauldronTelemetryEventSerializer
import avh.ckc.demo.serialization.OrderLifecycleEventSerializer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import kotlin.time.Duration.Companion.seconds

internal data class StaticTrafficWarmupParticipantPlan(
    val consumerId: WarmupConsumerId,
    val route: DemoConsumerRoute,
    val recipient: Boolean
)

internal data class StaticTrafficWarmupPlan(
    val order: StaticTrafficWarmupParticipantPlan,
    val batch: StaticTrafficWarmupParticipantPlan,
    val telemetry: StaticTrafficWarmupParticipantPlan
) {
    companion object {
        fun from(properties: DemoApplicationProperties): StaticTrafficWarmupPlan {
            val warmup = properties.trafficWarmup
            val participants = listOf(
                ParticipantInput(
                    DemoTopics.ORDER_EVENTS,
                    warmup.order,
                    properties.consumers.order,
                    OrderLifecycleEventSerializer::class.java
                ),
                ParticipantInput(
                    DemoTopics.BATCH_EVENTS,
                    warmup.batch,
                    properties.consumers.batch,
                    BatchLifecycleEventSerializer::class.java
                ),
                ParticipantInput(
                    DemoTopics.CAULDRON_EVENTS,
                    warmup.telemetry,
                    properties.consumers.telemetry,
                    CauldronTelemetryEventSerializer::class.java
                )
            )
            validate(warmup, participants, properties.audit.enabled)
            val plans = participants.map { participant -> participant.toPlan(properties, warmup) }
            return StaticTrafficWarmupPlan(plans[0], plans[1], plans[2])
        }

        private fun validate(
            warmup: DemoApplicationProperties.TrafficWarmup,
            participants: List<ParticipantInput>,
            auditEnabled: Boolean
        ) {
            if (warmup.role !in setOf(
                    DemoApplicationProperties.TrafficWarmupRole.DONOR,
                    DemoApplicationProperties.TrafficWarmupRole.RECIPIENT
                )) return
            WarmupSessionId(warmup.sessionId)
            require(AUDIT_FIELD_SEPARATOR !in warmup.sessionId) {
                "demo.traffic-warmup.session-id must not contain '$AUDIT_FIELD_SEPARATOR'"
            }
            WarmupForwardingProfile(
                initialFraction = warmup.initialFraction,
                targetFraction = warmup.targetFraction,
                rampUpDuration = warmup.rampUpSeconds.seconds
            )
            require(warmup.maxInFlightSends > 0) {
                "demo.traffic-warmup.max-in-flight-sends must be > 0"
            }
            require(warmup.recipientGroupIdPrefix.isNotBlank()) {
                "demo.traffic-warmup.recipient-group-id-prefix must not be blank"
            }
            val enabled = participants.filter { it.configuration.enabled }
            require(enabled.isNotEmpty()) { "At least one traffic warm-up participant must be enabled" }
            enabled.forEach {
                require(it.runtime.processingMode == ProcessingMode.AT_LEAST_ONCE_NO_ORDERING) {
                    "Traffic warm-up participant ${it.configuration.consumerId} must use AT_LEAST_ONCE_NO_ORDERING"
                }
                WarmupConsumerId(it.configuration.consumerId)
                require(AUDIT_FIELD_SEPARATOR !in it.configuration.consumerId) {
                    "Traffic warm-up consumer ID must not contain '$AUDIT_FIELD_SEPARATOR'"
                }
                require(it.configuration.topic.isNotBlank()) { "Traffic warm-up topic must not be blank" }
                require(it.configuration.partition >= 0) { "Traffic warm-up partition must not be negative" }
                require(!auditEnabled || auditTopicIdOrNull(it.configuration.topic) != null) {
                    "No audit topic ID is configured for traffic warm-up topic '${it.configuration.topic}'"
                }
            }
            val duplicateDestinations = enabled.groupingBy {
                TopicPartition(it.configuration.topic, it.configuration.partition)
            }.eachCount().filterValues { it > 1 }.keys
            require(duplicateDestinations.isEmpty()) {
                "Traffic warm-up partitions must be exclusive: " +
                    duplicateDestinations.sortedBy(TopicPartition::toString)
            }
        }
    }
}

private const val AUDIT_FIELD_SEPARATOR = '|'

private data class ParticipantInput(
    val sourceTopic: String,
    val configuration: DemoApplicationProperties.TrafficWarmupParticipant,
    val runtime: DemoApplicationProperties.ConsumerRuntime,
    val valueSerializer: Class<*>
) {
    fun toPlan(
        properties: DemoApplicationProperties,
        warmup: DemoApplicationProperties.TrafficWarmup
    ): StaticTrafficWarmupParticipantPlan {
        val consumerId = WarmupConsumerId(configuration.consumerId.ifBlank { sourceTopic })
        if (warmup.role !in setOf(
                DemoApplicationProperties.TrafficWarmupRole.DONOR,
                DemoApplicationProperties.TrafficWarmupRole.RECIPIENT
            ) || !configuration.enabled) {
            return StaticTrafficWarmupParticipantPlan(consumerId, DemoConsumerRoute.main(sourceTopic), false)
        }
        val destination = TopicPartition(configuration.topic, configuration.partition)
        if (warmup.role == DemoApplicationProperties.TrafficWarmupRole.RECIPIENT) {
            return StaticTrafficWarmupParticipantPlan(consumerId, DemoConsumerRoute.recipient(destination), true)
        }
        val producerProperties = properties.kafkaConnectionProperties(
            warmup.kafkaBootstrapServers.ifBlank { properties.kafka.bootstrapServers }
        ) + mapOf(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to valueSerializer
        )
        val forwarding = WarmupForwardingConfiguration(
            sessionId = WarmupSessionId(warmup.sessionId),
            consumerId = consumerId,
            assignments = listOf(
                WarmupPartitionAssignment(consumerId, sourceTopic, destination.topic(), destination.partition())
            ),
            profile = WarmupForwardingProfile(
                initialFraction = warmup.initialFraction,
                targetFraction = warmup.targetFraction,
                rampUpDuration = warmup.rampUpSeconds.seconds
            ),
            producerProperties = producerProperties,
            maxInFlightSends = warmup.maxInFlightSends
        )
        return StaticTrafficWarmupParticipantPlan(
            consumerId,
            DemoConsumerRoute.main(
                sourceTopic,
                forwarding,
                WarmupForwardingObserver { logWarmupForwarded(it, properties.audit) }
            ),
            false
        )
    }
}
