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

        val encoded = OffsetTrackerSerializer.serialize(snapshot)
        val decoded = OffsetTrackerSerializer.deserialize(encoded)

        assertEquals(0, encoded.first().toInt())
        assertSnapshotEquals(snapshot, decoded)
    }

    @Test
    fun `one kilobyte snapshot uses raw codec`() {
        val snapshot = OffsetTrackerSnapshot(
            headWordOffset = 10L,
            headWordIndex = 0,
            words = LongArray(128) { -1L }
        )

        val encoded = OffsetTrackerSerializer.serialize(snapshot)
        val decoded = OffsetTrackerSerializer.deserialize(encoded)

        assertEquals(0, encoded.first().toInt())
        assertSnapshotEquals(snapshot, decoded)
    }

    @Test
    fun `large compressible snapshot uses zstd codec`() {
        val snapshot = OffsetTrackerSnapshot(
            headWordOffset = 10L,
            headWordIndex = 0,
            words = LongArray(256) { -1L }
        )

        val encoded = OffsetTrackerSerializer.serialize(snapshot)
        val decoded = OffsetTrackerSerializer.deserialize(encoded)

        assertEquals(1, encoded.first().toInt())
        assertTrue(encoded.size < snapshot.words.size * Long.SIZE_BYTES)
        assertSnapshotEquals(snapshot, decoded)
    }

    private fun assertSnapshotEquals(expected: OffsetTrackerSnapshot, actual: OffsetTrackerSnapshot) {
        assertEquals(expected.headWordOffset, actual.headWordOffset)
        assertEquals(expected.headWordIndex, actual.headWordIndex)
        assertContentEquals(expected.words, actual.words)
    }
}
