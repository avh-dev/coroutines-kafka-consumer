package avh.ckc.core.warmup

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runInterruptible
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.common.TopicPartition
import java.time.Duration as JavaDuration
import java.util.concurrent.TimeUnit
import kotlin.time.Duration

/** Resolves each warm-up topic through the Kafka connection configured for its participant. */
internal class KafkaWarmupDestinationMetadata(
    participantConnections: Map<WarmupParticipantId, Map<String, Any?>>,
    private val timeout: Duration,
    private val dispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val adminFactory: (Map<String, Any?>) -> Admin = AdminClient::create
) : WarmupDestinationMetadata {
    private val participantConnections = participantConnections.mapValues { (_, properties) -> properties.toMap() }

    init {
        require(timeout.isPositive() && timeout.inWholeMilliseconds > 0) {
            "Warm-up metadata timeout must be at least one millisecond"
        }
    }

    override suspend fun endOffsets(mapping: WarmupTopicMapping): Map<Int, Long> = runInterruptible(dispatcher) {
        val properties = requireNotNull(participantConnections[mapping.participantId]) {
            "No warm-up Kafka connection configured for participant ${mapping.participantId.value}"
        }
        val admin = adminFactory(properties)
        try {
            val timeoutMillis = timeout.inWholeMilliseconds
            val description = admin.describeTopics(listOf(mapping.warmupTopic))
                .allTopicNames().get(timeoutMillis, TimeUnit.MILLISECONDS)
                .getValue(mapping.warmupTopic)
            val partitions = description.partitions().map { TopicPartition(mapping.warmupTopic, it.partition()) }
            admin.listOffsets(partitions.associateWith { OffsetSpec.latest() })
                .all().get(timeoutMillis, TimeUnit.MILLISECONDS)
                .mapKeys { (partition, _) -> partition.partition() }
                .mapValues { (_, result) -> result.offset() }
        } finally {
            admin.close(JavaDuration.ofMillis(timeout.inWholeMilliseconds))
        }
    }
}
