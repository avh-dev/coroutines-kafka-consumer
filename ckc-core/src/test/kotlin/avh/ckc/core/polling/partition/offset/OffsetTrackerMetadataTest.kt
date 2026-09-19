package avh.ckc.core.polling.partition.offset

import org.apache.kafka.common.TopicPartition
import java.util.Base64
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class OffsetTrackerMetadataTest {

    @Test
    fun `metadata round trips snapshot through versioned context-bound envelope`() {
        val snapshot = OffsetTrackerSnapshot(
            headWordOffset = 64L,
            headWordIndex = 1,
            words = longArrayOf(0L, 0b101L, -1L, 0L)
        )

        val metadata = assertNotNull(OffsetTrackerMetadata.encode(snapshot, context()))
        val decoded = OffsetTrackerMetadata.decode(metadata, context())

        assertTrue(metadata.startsWith("ckc:v2:"))
        assertEquals(snapshot.headWordOffset, decoded.headWordOffset)
        assertEquals(snapshot.headWordIndex, decoded.headWordIndex)
        assertContentEquals(snapshot.words, decoded.words)
    }

    @Test
    fun `metadata encode returns null when encoded string exceeds the configured limit`() {
        val snapshot = OffsetTrackerSnapshot(
            headWordOffset = 0L,
            headWordIndex = 0,
            words = LongArray(256) { it.toLong() }
        )

        assertNull(OffsetTrackerMetadata.encode(snapshot, context(), maxMetadataBytes = 8))
    }

    @Test
    fun `metadata is rejected when committed offset changes`() {
        val metadata = assertNotNull(OffsetTrackerMetadata.encode(snapshot(), context(committedOffset = 42L)))

        assertFailsWith<IllegalArgumentException> {
            OffsetTrackerMetadata.decode(metadata, context(committedOffset = 21L))
        }
    }

    @Test
    fun `metadata is rejected when consumer group or topic partition changes`() {
        val metadata = assertNotNull(OffsetTrackerMetadata.encode(snapshot(), context()))
        val foreignContexts = listOf(
            context(groupId = "other-group"),
            context(topicPartition = TopicPartition("other-topic", 3)),
            context(topicPartition = TopicPartition("topic-a", 4))
        )

        foreignContexts.forEach { foreignContext ->
            assertFailsWith<IllegalArgumentException> {
                OffsetTrackerMetadata.decode(metadata, foreignContext)
            }
        }
    }

    @Test
    fun `metadata is rejected when checksum is corrupted`() {
        val metadata = assertNotNull(OffsetTrackerMetadata.encode(snapshot(), context()))
        val envelope = Base64.getUrlDecoder().decode(metadata.removePrefix(OffsetTrackerMetadata.PREFIX))
        envelope[envelope.lastIndex] = (envelope.last().toInt() xor 1).toByte()
        val corrupted = OffsetTrackerMetadata.PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(envelope)

        assertFailsWith<IllegalArgumentException> {
            OffsetTrackerMetadata.decode(corrupted, context())
        }
    }

    @Test
    fun `legacy unprefixed metadata is rejected`() {
        val legacyMetadata = Base64.getUrlEncoder().withoutPadding()
            .encodeToString(OffsetTrackerSerializer.serialize(snapshot()))

        assertFailsWith<IllegalArgumentException> {
            OffsetTrackerMetadata.decode(legacyMetadata, context())
        }
    }

    @Test
    fun `metadata with unsupported CKC version is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            OffsetTrackerMetadata.decode("ckc:v3:payload", context())
        }
    }

    private fun snapshot() = OffsetTrackerSnapshot(
        headWordOffset = 0L,
        headWordIndex = 0,
        words = longArrayOf(0b101L, 0L)
    )

    private fun context(
        groupId: String = "test-group",
        topicPartition: TopicPartition = TopicPartition("topic-a", 3),
        committedOffset: Long = 42L
    ) = OffsetTrackerMetadataContext(groupId, topicPartition, committedOffset)
}
