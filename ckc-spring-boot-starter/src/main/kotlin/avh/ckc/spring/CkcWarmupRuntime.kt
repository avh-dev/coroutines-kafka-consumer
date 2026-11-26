package avh.ckc.spring

import avh.ckc.core.CoroutinesKafkaConsumerBuilder
import avh.ckc.warmup.TrafficWarmupLifecycleEvent
import avh.ckc.warmup.TrafficWarmupLifecyclePhase
import avh.ckc.warmup.trafficWarmup
import avh.ckc.warmup.trafficWarmupContext
import java.util.concurrent.ConcurrentHashMap
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.springframework.context.ApplicationEventPublisher
import kotlin.time.toKotlinDuration

internal fun resolveWarmupParticipants(
    properties: CkcConsumerProperties,
    events: ApplicationEventPublisher
): Map<String, CkcWarmupParticipant> {
    if (!properties.warmup.enabled) return emptyMap()

    val contexts = mutableMapOf<String, CkcWarmupContext>()
    return buildMap {
        properties.consumers.forEach { (name, consumer) ->
            val participant = consumer.warmup ?: return@forEach
            val path = "ckc.consumers.$name.warmup"
            require(consumer.topicPattern.isNullOrBlank()) {
                "$path requires explicit topics and does not support topic-pattern"
            }
            require(participant.context.isNotBlank()) { "$path.context must be specified" }
            val contextSettings = requireNotNull(properties.warmup.contexts[participant.context]) {
                "$path references unknown warm-up context '${participant.context}'"
            }
            val context = contexts.getOrPut(participant.context) {
                CkcWarmupContext(participant.context, contextSettings, properties.clusters, events)
            }
            require(participant.mappings.isNotEmpty()) { "$path.mappings must contain at least one mapping" }
            participant.mappings.forEachIndexed { index, mapping ->
                require(mapping.sourceTopic.isNotBlank() && mapping.sourceTopic in consumer.topics) {
                    "$path.mappings[$index].source-topic must belong to the main subscription"
                }
                require(mapping.warmupTopic.isNotBlank()) {
                    "$path.mappings[$index].warmup-topic must be specified"
                }
                context.registerDestination(name, mapping.warmupTopic)
            }
            require(participant.keySerializer.isNotBlank() && participant.valueSerializer.isNotBlank()) {
                "$path requires key-serializer and value-serializer"
            }
            val destination = warmupCluster(properties.clusters, participant.destinationCluster, "$path.destination-cluster")
            put(name, CkcWarmupParticipant(name, participant, context, destination, events))
        }
    }
}

private fun warmupCluster(
    clusters: Map<String, CkcConsumerProperties.Cluster>,
    name: String,
    path: String
): Map<String, String> {
    require(name.isNotBlank()) { "$path must be specified" }
    val kafka = requireNotNull(clusters[name]) { "$path references unknown cluster '$name'" }.kafkaProperties
    require(!kafka[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG].isNullOrBlank()) {
        "$path references cluster '$name' without bootstrap.servers"
    }
    return kafka
}

internal class CkcWarmupContext(
    val name: String,
    settings: CkcWarmupProperties.Context,
    clusters: Map<String, CkcConsumerProperties.Cluster>,
    events: ApplicationEventPublisher
) {
    private val destinations = mutableMapOf<String, String>()
    private val participantPhases = ConcurrentHashMap<String, TrafficWarmupLifecyclePhase>()

    @Volatile
    private var contextPhase: TrafficWarmupLifecyclePhase? = null

    val runtime = try {
        val control = warmupCluster(clusters, settings.controlCluster, "control-cluster")
        trafficWarmupContext {
            settings.instanceId?.let { instanceId = it }
            namespace = settings.namespace
            controlTopic = settings.controlTopic
            controlConsumerProperties = control
            controlProducerProperties = control
            settings.warmupDuration?.let { warmupDuration = it.toKotlinDuration() }
            settings.offerWindow?.let { offerWindow = it.toKotlinDuration() }
            settings.participantStartTimeout?.let { participantStartTimeout = it.toKotlinDuration() }
            settings.preparationTimeout?.let { preparationTimeout = it.toKotlinDuration() }
            settings.completionTimeout?.let { completionTimeout = it.toKotlinDuration() }
            settings.delegationTimeout?.let { delegationTimeout = it.toKotlinDuration() }
            settings.donorLivenessTimeout?.let { donorLivenessTimeout = it.toKotlinDuration() }
            settings.maxDonorSessions?.let { maxDonorSessions = it }
            settings.maxAggregateQuotaPerSecond?.let { maxAggregateQuotaPerSecond = it }
            settings.maxInflightDelegations?.let { maxInflightDelegations = it }
            settings.serviceThreadLimit?.let { serviceThreadLimit = it }
            settings.serviceThreadIdleTimeout?.let { serviceThreadIdleTimeout = it.toKotlinDuration() }
            settings.brokerLifecycle?.takeIf { it.topic.isNotBlank() }?.let { broker ->
                brokerLifecycle {
                    topic = broker.topic
                    kafkaProperties = broker.cluster?.let { warmupCluster(clusters, it, "broker-lifecycle.cluster") }
                        ?: control
                    broker.operationTimeout?.let { operationTimeout = it.toKotlinDuration() }
                }
            }
            onLifecycleEvent { event ->
                recordPhase(event)
                events.publishEvent(CkcWarmupLifecycleEvent(name, event))
            }
        }
    } catch (error: IllegalArgumentException) {
        throw IllegalArgumentException("Invalid ckc.warmup.contexts.$name: ${error.message}", error)
    }

    fun registerDestination(consumer: String, topic: String) {
        val previous = destinations.putIfAbsent(topic, consumer)
        require(previous == null) {
            "ckc.consumers.$consumer.warmup maps destination '$topic' already used by '$previous' in context '$name'"
        }
    }

    private fun recordPhase(event: TrafficWarmupLifecycleEvent) {
        val participant = event.participantId
        if (participant == null) {
            contextPhase = event.phase
        } else {
            participantPhases.compute(participant) { _, previous ->
                if (previous == TrafficWarmupLifecyclePhase.PARTICIPANT_STOPPED) previous else event.phase
            }
        }
    }

    fun snapshot(consumer: String): CkcWarmupStateSnapshot {
        val participantPhase = participantPhases[consumer]
        val phase = when (participantPhase) {
            null -> "REGISTERED"
            TrafficWarmupLifecyclePhase.WAITING_FOR_PARTICIPANTS -> (contextPhase ?: participantPhase).name
            else -> participantPhase.name
        }
        return CkcWarmupStateSnapshot(name, phase, participantPhase == TrafficWarmupLifecyclePhase.MAIN_READY)
    }
}

internal class CkcWarmupParticipant(
    private val name: String,
    private val settings: CkcWarmupProperties.Participant,
    private val context: CkcWarmupContext,
    private val destination: Map<String, String>,
    private val events: ApplicationEventPublisher
) {
    fun configure(builder: CoroutinesKafkaConsumerBuilder<Any?, Any?>) {
        builder.trafficWarmup(context.runtime, name) {
            warmupConsumerProperties = destination + settings.consumerProperties
            forwardingProducerProperties = destination + settings.producerProperties + mapOf(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to settings.keySerializer,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to settings.valueSerializer
            )
            settings.mappings.forEach { mapping ->
                mapTopic(mapping.sourceTopic, mapping.warmupTopic) {
                    mapping.quotaProfile?.let { quotaProfile = it }
                    mapping.quotaInterval?.let { quotaInterval = it.toKotlinDuration() }
                }
            }
            onForwarded { event -> events.publishEvent(CkcWarmupForwardedEvent(context.name, name, event)) }
        }
    }

    fun snapshot(): CkcWarmupStateSnapshot = context.snapshot(name)
}

internal data class CkcWarmupStateSnapshot(val context: String, val phase: String, val mainReady: Boolean)
