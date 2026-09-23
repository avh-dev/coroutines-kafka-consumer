package avh.ckc.core.polling.partition.offset

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
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

    private fun assertSnapshotEquals(expected: OffsetTrackerSnapshot, actual: OffsetTrackerSnapshot) {
        assertEquals(expected.headWordOffset, actual.headWordOffset)
        assertEquals(expected.headWordIndex, actual.headWordIndex)
        assertContentEquals(expected.words, actual.words)
    }
}
