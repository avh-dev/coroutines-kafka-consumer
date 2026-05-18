package avh.ckc.core.offset.compression

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class OffsetMetadataCompressionTest {

    @Test
    fun `all codecs round trip representative snapshots`() {
        val snapshots = listOf(
            snapshot(bitCount = 0),
            snapshot(bitCount = 8) { it in setOf(0, 1, 2, 7) },
            snapshot(bitCount = 129) { it % 3 != 0 },
            snapshot(bitCount = 1024) { it in 32..96 || it in 700..730 },
            snapshot(bitCount = 4096) { false },
            snapshot(bitCount = 4096) { true }
        )

        for (codec in offsetMetadataCodecs()) {
            for (snapshot in snapshots) {
                val decoded = codec.decode(codec.encode(snapshot))

                assertEquals(snapshot.baseOffset, decoded.baseOffset, codec.name)
                assertEquals(snapshot.bitCount, decoded.bitCount, codec.name)
                assertContentEquals(snapshot.normalizedWords(), decoded.normalizedWords(), codec.name)
            }
        }
    }

    @Test
    fun `word RLE is compact for repeated words`() {
        val snapshot = snapshot(bitCount = 4096) { false }
        val rawSize = RawOffsetMetadataCodec.encode(snapshot).size
        val rleSize = WordRunLengthOffsetMetadataCodec.encode(snapshot).size

        assertTrue(rleSize < rawSize / 4, "word RLE should compress repeated zero words")
    }

    @Test
    fun `bit RLE is compact for clustered gaps`() {
        val snapshot = snapshot(bitCount = 4096) { it in 0..1023 || it in 3000..3500 }
        val rawSize = RawOffsetMetadataCodec.encode(snapshot).size
        val rleSize = BitRunLengthOffsetMetadataCodec.encode(snapshot).size

        assertTrue(rleSize < rawSize / 4, "bit RLE should compress long bit runs")
    }

    @Test
    fun `byte RLE supports adjacent zero and ff runs through empty literals`() {
        val snapshot = OffsetBitsetSnapshot(
            baseOffset = 1_000L,
            bitCount = 64,
            words = longArrayOf(-1L shl 32)
        )

        val decoded = ByteRunLengthOffsetMetadataCodec().decode(ByteRunLengthOffsetMetadataCodec().encode(snapshot))

        assertContentEquals(snapshot.normalizedWords(), decoded.normalizedWords())
    }

    @Test
    fun `byte RLE keeps short special byte groups in literals`() {
        val snapshot = OffsetBitsetSnapshot(
            baseOffset = 1_000L,
            bitCount = 16,
            words = longArrayOf(0xffffL)
        )
        val minThreeSize = ByteRunLengthOffsetMetadataCodec(minRunLength = 3).encode(snapshot).size
        val minTwoSize = ByteRunLengthOffsetMetadataCodec(minRunLength = 2).encode(snapshot).size

        assertTrue(minThreeSize > minTwoSize, "two 0xff bytes should remain literal when minRunLength is 3")
    }

    @Test
    fun `commit metadata experiment snapshot has stable codec sizes`() {
        val snapshot = commitMetadataExperimentSnapshot()
        val sizes = offsetMetadataCodecs().associate { codec -> codec.name to codec.encode(snapshot).size }

        assertEquals(16 * 1024 * 8, snapshot.bitCount)
        assertEquals(16_396, sizes.getValue("raw"))
        assertEquals(17_013, sizes.getValue("wordRle"))
        assertEquals(7_849, sizes.getValue("bitRle"))
        assertEquals(6_844, sizes.getValue("byteRle"))
        assertEquals(7_201, sizes.getValue("byteRleNoLiteralLength"))
        assertEquals(4_548, sizes.getValue("zstd"))
        assertEquals(7_706, sizes.getValue("lz4"))
    }

    private fun snapshot(
        baseOffset: Long = 1_000L,
        bitCount: Int,
        isProcessed: (Int) -> Boolean = { false }
    ): OffsetBitsetSnapshot {
        val words = LongArray(wordsForBits(bitCount))
        for (bitIndex in 0 until bitCount) {
            if (isProcessed(bitIndex)) {
                words[bitIndex ushr 6] = words[bitIndex ushr 6] or (1L shl bitIndex)
            }
        }
        return OffsetBitsetSnapshot(baseOffset, bitCount, words)
    }
}
