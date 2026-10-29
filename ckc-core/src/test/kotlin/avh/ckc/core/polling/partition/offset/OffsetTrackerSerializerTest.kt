package avh.ckc.core.polling.partition.offset

import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class OffsetTrackerSerializerTest {

    @Test
    fun `small snapshot uses raw codec`() {
        val snapshot = OffsetTrackerSnapshot(
            headWordOffset = 10L,
            headWordIndex = 1,
            words = longArrayOf(0L, 0b1011L)
        )

        val serialization = OffsetTrackerSerializer.serialize(snapshot)
        val decoded = OffsetTrackerSerializer.deserialize(serialization.bytes)

        assertEquals(OffsetTrackerCompression.NONE, serialization.payload.compression)
        assertEquals(16, serialization.payload.rawPayloadSizeBytes)
        assertEquals(16, serialization.payload.encodedPayloadSizeBytes)
        assertSnapshotEquals(snapshot, decoded)
    }

    @Test
    fun `one kilobyte snapshot uses raw codec`() {
        val snapshot = OffsetTrackerSnapshot(
            headWordOffset = 10L,
            headWordIndex = 0,
            words = LongArray(128) { -1L }
        )

        val serialization = OffsetTrackerSerializer.serialize(snapshot)
        val decoded = OffsetTrackerSerializer.deserialize(serialization.bytes)

        assertEquals(OffsetTrackerCompression.NONE, serialization.payload.compression)
        assertEquals(1024, serialization.payload.rawPayloadSizeBytes)
        assertEquals(1024, serialization.payload.encodedPayloadSizeBytes)
        assertSnapshotEquals(snapshot, decoded)
    }

    @Test
    fun `large compressible snapshot uses zstd codec`() {
        val snapshot = OffsetTrackerSnapshot(
            headWordOffset = 10L,
            headWordIndex = 0,
            words = LongArray(256) { -1L }
        )

        val serialization = OffsetTrackerSerializer.serialize(snapshot)
        val decoded = OffsetTrackerSerializer.deserialize(serialization.bytes)

        assertEquals(OffsetTrackerCompression.ZSTD, serialization.payload.compression)
        assertEquals(2048, serialization.payload.rawPayloadSizeBytes)
        assertTrue(serialization.payload.encodedPayloadSizeBytes < serialization.payload.rawPayloadSizeBytes)
        assertSnapshotEquals(snapshot, decoded)
    }

    @Test
    fun `declared snapshot size is bounded before zstd decompression`() {
        val serialization = OffsetTrackerSerializer.serialize(
            OffsetTrackerSnapshot(0L, 0, LongArray(256) { -1L })
        )
        assertEquals(OffsetTrackerCompression.ZSTD, serialization.payload.compression)
        ByteBuffer.wrap(serialization.bytes).order(ByteOrder.LITTLE_ENDIAN)
            .putInt(WORD_COUNT_OFFSET, Int.MAX_VALUE)

        assertFailsWith<IllegalArgumentException> {
            OffsetTrackerSerializer.deserialize(serialization.bytes, maxCapacityBits = 128)
        }
    }

    @Test
    fun `snapshot larger than configured tracker limit is rejected`() {
        val serialization = OffsetTrackerSerializer.serialize(
            OffsetTrackerSnapshot(0L, 0, LongArray(4))
        )

        assertFailsWith<IllegalArgumentException> {
            OffsetTrackerSerializer.deserialize(serialization.bytes, maxCapacityBits = 128)
        }
    }

    @Test
    fun `malformed ring structure is rejected`() {
        val invalidWordCount = OffsetTrackerSerializer.serialize(
            OffsetTrackerSnapshot(0L, 0, LongArray(4))
        ).bytes.also {
            ByteBuffer.wrap(it).order(ByteOrder.LITTLE_ENDIAN).putInt(WORD_COUNT_OFFSET, 3)
        }
        val invalidHeadIndex = OffsetTrackerSerializer.serialize(
            OffsetTrackerSnapshot(0L, 0, LongArray(2))
        ).bytes.also {
            ByteBuffer.wrap(it).order(ByteOrder.LITTLE_ENDIAN).putInt(HEAD_WORD_INDEX_OFFSET, 2)
        }

        assertFailsWith<IllegalArgumentException> {
            OffsetTrackerSerializer.deserialize(invalidWordCount)
        }
        assertFailsWith<IllegalArgumentException> {
            OffsetTrackerSerializer.deserialize(invalidHeadIndex)
        }
    }

    private fun assertSnapshotEquals(expected: OffsetTrackerSnapshot, actual: OffsetTrackerSnapshot) {
        assertEquals(expected.headWordOffset, actual.headWordOffset)
        assertEquals(expected.headWordIndex, actual.headWordIndex)
        assertContentEquals(expected.words, actual.words)
    }

    private companion object {
        const val HEAD_WORD_INDEX_OFFSET = Byte.SIZE_BYTES + Long.SIZE_BYTES
        const val WORD_COUNT_OFFSET = HEAD_WORD_INDEX_OFFSET + Int.SIZE_BYTES
    }
}
