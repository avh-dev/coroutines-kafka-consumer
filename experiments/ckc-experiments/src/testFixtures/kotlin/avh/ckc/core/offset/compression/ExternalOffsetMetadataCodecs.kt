package avh.ckc.core.offset.compression

import com.github.luben.zstd.Zstd
import net.jpountz.lz4.LZ4Factory
import java.io.ByteArrayOutputStream

object ZstdOffsetMetadataCodec : OffsetMetadataCodec {
    override val name: String = "zstd"

    override fun encode(snapshot: OffsetBitsetSnapshot): ByteArray {
        val bytes = snapshot.normalizedWords().toLittleEndianBytes(byteCountForBits(snapshot.bitCount))
        val compressed = Zstd.compress(bytes)
        val out = ByteArrayOutputStream(HEADER_BYTES + compressed.size)
        out.writeHeader(snapshot.baseOffset, snapshot.bitCount)
        out.write(compressed)
        return out.toByteArray()
    }

    override fun decode(payload: ByteArray): OffsetBitsetSnapshot {
        val reader = PayloadReader(payload)
        val header = reader.readHeader()
        val bytes = Zstd.decompress(reader.readRemainingBytes(), byteCountForBits(header.bitCount))
        return OffsetBitsetSnapshot(
            baseOffset = header.baseOffset,
            bitCount = header.bitCount,
            words = bytes.toLittleEndianWords()
        )
    }
}

object Lz4OffsetMetadataCodec : OffsetMetadataCodec {
    override val name: String = "lz4"

    private val factory = LZ4Factory.fastestInstance()
    private val compressor = factory.fastCompressor()
    private val decompressor = factory.fastDecompressor()

    override fun encode(snapshot: OffsetBitsetSnapshot): ByteArray {
        val bytes = snapshot.normalizedWords().toLittleEndianBytes(byteCountForBits(snapshot.bitCount))
        val compressed = ByteArray(compressor.maxCompressedLength(bytes.size))
        val compressedSize = compressor.compress(bytes, 0, bytes.size, compressed, 0, compressed.size)
        val out = ByteArrayOutputStream(HEADER_BYTES + compressedSize)
        out.writeHeader(snapshot.baseOffset, snapshot.bitCount)
        out.write(compressed, 0, compressedSize)
        return out.toByteArray()
    }

    override fun decode(payload: ByteArray): OffsetBitsetSnapshot {
        val reader = PayloadReader(payload)
        val header = reader.readHeader()
        val bytes = ByteArray(byteCountForBits(header.bitCount))
        val compressed = reader.readRemainingBytes()
        decompressor.decompress(compressed, 0, bytes, 0, bytes.size)
        return OffsetBitsetSnapshot(
            baseOffset = header.baseOffset,
            bitCount = header.bitCount,
            words = bytes.toLittleEndianWords()
        )
    }
}
