package avh.ckc.core.offset

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class OffsetTrackerMetadataTest {

    @Test
    fun `metadata round trips snapshot through base64 string`() {
        val snapshot = OffsetTrackerSnapshot(
            headWordOffset = 64L,
            headWordIndex = 1,
            words = longArrayOf(0L, 0b101L, -1L, 0L)
        )

        val metadata = assertNotNull(OffsetTrackerMetadata.encode(snapshot))
        val decoded = OffsetTrackerMetadata.decode(metadata)

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

        assertNull(OffsetTrackerMetadata.encode(snapshot, maxMetadataBytes = 8))
    }
}
