package avh.ckc.demo.consumer.ckc

import avh.ckc.core.ProcessingMode
import avh.ckc.core.warmup.TrafficWarmupContext
import avh.ckc.core.warmup.WarmupConsumerId
import avh.ckc.core.warmup.WarmupForwardingObserver
import avh.ckc.core.warmup.WarmupForwardingProfile
import avh.ckc.core.warmup.WarmupSessionId
import avh.ckc.core.warmup.trafficWarmupContext
import avh.ckc.demo.DemoTopics
import avh.ckc.demo.audit.auditTopicIdOrNull
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.config.kafkaConnectionProperties
import avh.ckc.demo.logWarmupForwarded
import avh.ckc.demo.serialization.BatchLifecycleEventSerializer
import avh.ckc.demo.serialization.CauldronTelemetryEventSerializer
import avh.ckc.demo.serialization.OrderLifecycleEventSerializer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import io.micrometer.core.instrument.MeterRegistry
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

internal data class AutomaticTrafficWarmupParticipantPlan(
    val context: TrafficWarmupContext,
    val consumerId: String,
    val sourceTopic: String,
    val warmupTopic: String,
    val warmupConnectionProperties: Map<String, Any>,
    val forwardingProducerProperties: Map<String, Any>,
    val forwardingProfile: WarmupForwardingProfile,
    val forwardingObserver: (WarmupSessionId) -> WarmupForwardingObserver
)

internal data class AutomaticTrafficWarmupPlan(
    val order: AutomaticTrafficWarmupParticipantPlan?,
    val batch: AutomaticTrafficWarmupParticipantPlan?,
    val telemetry: AutomaticTrafficWarmupParticipantPlan?
) {
    companion object {
        fun from(
            properties: DemoApplicationProperties,
            donorLoadScore: () -> Double? = { null }
        ): AutomaticTrafficWarmupPlan? {
            val warmup = properties.trafficWarmup
            if (warmup.role != DemoApplicationProperties.TrafficWarmupRole.AUTOMATIC) return null

            val dataBootstrapServers = warmup.kafkaBootstrapServers.ifBlank {
                properties.kafka.bootstrapServers
            }
            val warmupConnection = properties.kafkaConnectionProperties(dataBootstrapServers)
            val context = trafficWarmupContext {
                instanceId = warmup.instanceId
                controlTopic = warmup.controlTopic
                kafkaProperties = warmupConnection
                donorSearchTimeout = warmup.donorSearchTimeoutSeconds.seconds
                stageTimeout = warmup.stageTimeoutSeconds.seconds
                heartbeatInterval = warmup.heartbeatIntervalMs.milliseconds
                electionTimeout = warmup.electionTimeoutMs.milliseconds
                this.donorLoadScore = donorLoadScore
            }
            val profile = WarmupForwardingProfile(
                initialFraction = warmup.initialFraction,
                targetFraction = warmup.targetFraction,
                rampUpDuration = warmup.rampUpSeconds.seconds,
                holdDuration = warmup.holdSeconds.seconds
            )
            val inputs = listOf(
                AutomaticParticipantInput(
                    DemoTopics.ORDER_EVENTS,
                    warmup.order,
                    properties.consumers.order,
                    OrderLifecycleEventSerializer::class.java
                ),
                AutomaticParticipantInput(
                    DemoTopics.BATCH_EVENTS,
                    warmup.batch,
                    properties.consumers.batch,
                    BatchLifecycleEventSerializer::class.java
                ),
                AutomaticParticipantInput(
                    DemoTopics.CAULDRON_EVENTS,
                    warmup.telemetry,
                    properties.consumers.telemetry,
                    CauldronTelemetryEventSerializer::class.java
                )
            )
            val enabled = inputs.filter { it.configuration.enabled }
            require(enabled.isNotEmpty()) { "At least one traffic warm-up participant must be enabled" }
            enabled.forEach { input -> input.validate(properties.audit.enabled) }

            val plans = inputs.map { input ->
                input.takeIf { it.configuration.enabled }?.toPlan(
                    context, warmupConnection, profile, properties
                )
            }
            return AutomaticTrafficWarmupPlan(plans[0], plans[1], plans[2])
        }
    }
}

private data class AutomaticParticipantInput(
    val sourceTopic: String,
    val configuration: DemoApplicationProperties.TrafficWarmupParticipant,
    val runtime: DemoApplicationProperties.ConsumerRuntime,
    val valueSerializer: Class<*>
) {
    fun validate(auditEnabled: Boolean) {
        require(runtime.processingMode == ProcessingMode.AT_LEAST_ONCE_NO_ORDERING) {
            "Traffic warm-up participant ${configuration.consumerId} must use AT_LEAST_ONCE_NO_ORDERING"
        }
        WarmupConsumerId(configuration.consumerId)
        require(AUTOMATIC_AUDIT_FIELD_SEPARATOR !in configuration.consumerId) {
            "Traffic warm-up consumer ID must not contain '$AUTOMATIC_AUDIT_FIELD_SEPARATOR'"
        }
        require(configuration.topic.isNotBlank()) { "Traffic warm-up topic must not be blank" }
        require(!auditEnabled || auditTopicIdOrNull(configuration.topic) != null) {
            "No audit topic ID is configured for traffic warm-up topic '${configuration.topic}'"
        }
    }

    fun toPlan(
        context: TrafficWarmupContext,
        warmupConnection: Map<String, Any>,
        profile: WarmupForwardingProfile,
        properties: DemoApplicationProperties
    ): AutomaticTrafficWarmupParticipantPlan = AutomaticTrafficWarmupParticipantPlan(
        context = context,
        consumerId = configuration.consumerId,
        sourceTopic = sourceTopic,
        warmupTopic = configuration.topic,
        warmupConnectionProperties = warmupConnection,
        forwardingProducerProperties = warmupConnection + mapOf(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to valueSerializer
        ),
        forwardingProfile = profile,
        forwardingObserver = { sessionId ->
            WarmupForwardingObserver { acknowledgement ->
                check(acknowledgement.sessionId == sessionId)
                logWarmupForwarded(acknowledgement, properties.audit)
            }
        }
    )
}

internal fun processCpuLoadScore(meterRegistry: MeterRegistry): () -> Double? = {
    meterRegistry.find(PROCESS_CPU_USAGE_METRIC).gauge()?.value()?.takeIf {
        it.isFinite() && it in 0.0..1.0
    }
}

private const val AUTOMATIC_AUDIT_FIELD_SEPARATOR = '|'
private const val PROCESS_CPU_USAGE_METRIC = "process.cpu.usage"
