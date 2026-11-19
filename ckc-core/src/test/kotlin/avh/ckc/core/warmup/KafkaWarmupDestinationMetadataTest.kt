package avh.ckc.core.warmup

import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.DescribeTopicsResult
import org.apache.kafka.clients.admin.ListOffsetsResult
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.TopicPartitionInfo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration as JavaDuration
import kotlin.time.Duration.Companion.seconds

class KafkaWarmupDestinationMetadataTest {
    @Test
    fun `metadata client uses the context service dispatcher`() = runBlocking {
        val participant = WarmupParticipantId("orders")
        val mapping = WarmupTopicMapping(participant, "orders", "warmup-orders", "fingerprint")
        val admin = metadataAdmin(mapping, 42)
        val execution = WarmupExecutionDispatchers("metadata", 3, 5.seconds)
        var factoryThread = ""
        val metadata = KafkaWarmupDestinationMetadata(
            participantConnections = mapOf(participant to mapOf("bootstrap.servers" to "unused:9092")),
            timeout = 2.seconds,
            dispatcher = execution.blocking,
            adminFactory = {
                factoryThread = Thread.currentThread().name
                admin
            }
        )
        try {
            assertEquals(mapOf(0 to 42L), metadata.endOffsets(mapping))
            assertTrue(factoryThread.startsWith("ckc-warmup-metadata-"))
        } finally {
            assertTrue(execution.close(5.seconds))
        }
    }

    @Test
    fun `mapping metadata uses only its participant connection`() = runBlocking {
        val participant = WarmupParticipantId("orders")
        val mapping = WarmupTopicMapping(participant, "orders", "warmup-orders", "fingerprint")
        val admin = mock<Admin>()
        val describe = mock<DescribeTopicsResult>()
        val offsets = mock<ListOffsetsResult>()
        val topicPartition = TopicPartition(mapping.warmupTopic, 0)
        whenever(admin.describeTopics(listOf(mapping.warmupTopic))).thenReturn(describe)
        whenever(describe.allTopicNames()).thenReturn(
            KafkaFuture.completedFuture(
                mapOf(
                    mapping.warmupTopic to TopicDescription(
                        mapping.warmupTopic,
                        false,
                        listOf(TopicPartitionInfo(0, null, emptyList(), emptyList()))
                    )
                )
            )
        )
        whenever(admin.listOffsets(any())).thenReturn(offsets)
        whenever(offsets.all()).thenReturn(
            KafkaFuture.completedFuture(
                mapOf(topicPartition to ListOffsetsResult.ListOffsetsResultInfo(42, -1, java.util.Optional.empty()))
            )
        )
        var usedProperties: Map<String, Any?>? = null
        val metadata = KafkaWarmupDestinationMetadata(
            participantConnections = mapOf(
                participant to mapOf("bootstrap.servers" to "participant:9092"),
                WarmupParticipantId("foreign") to mapOf("bootstrap.servers" to "foreign:9092")
            ),
            timeout = 2.seconds,
            adminFactory = { properties ->
                usedProperties = properties
                admin
            }
        )

        assertEquals(mapOf(0 to 42L), metadata.endOffsets(mapping))
        assertEquals("participant:9092", usedProperties?.get("bootstrap.servers"))
        verify(admin).close(any<JavaDuration>())
    }

    @Test
    fun `missing participant connection fails before opening an admin client`() {
        val mapping = WarmupTopicMapping(
            WarmupParticipantId("orders"),
            "orders",
            "warmup-orders",
            "fingerprint"
        )
        var factoryCalls = 0
        val metadata = KafkaWarmupDestinationMetadata(
            participantConnections = emptyMap(),
            timeout = 2.seconds,
            adminFactory = {
                factoryCalls++
                mock()
            }
        )

        assertThrows(IllegalArgumentException::class.java) {
            runBlocking { metadata.endOffsets(mapping) }
        }
        assertEquals(0, factoryCalls)
    }

    private fun metadataAdmin(mapping: WarmupTopicMapping, offset: Long): Admin {
        val admin = mock<Admin>()
        val describe = mock<DescribeTopicsResult>()
        val offsets = mock<ListOffsetsResult>()
        val topicPartition = TopicPartition(mapping.warmupTopic, 0)
        whenever(admin.describeTopics(listOf(mapping.warmupTopic))).thenReturn(describe)
        whenever(describe.allTopicNames()).thenReturn(
            KafkaFuture.completedFuture(
                mapOf(
                    mapping.warmupTopic to TopicDescription(
                        mapping.warmupTopic,
                        false,
                        listOf(TopicPartitionInfo(0, null, emptyList(), emptyList()))
                    )
                )
            )
        )
        whenever(admin.listOffsets(any())).thenReturn(offsets)
        whenever(offsets.all()).thenReturn(
            KafkaFuture.completedFuture(
                mapOf(topicPartition to ListOffsetsResult.ListOffsetsResultInfo(offset, -1, java.util.Optional.empty()))
            )
        )
        return admin
    }
}
