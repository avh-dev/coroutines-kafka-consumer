package avh.ckc.core.offset.compression

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Warmup
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.TimeUnit

@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
open class OffsetMetadataCompressionBenchmark {

    @Param("raw", "wordRle", "bitRle", "byteRle", "byteRleNoLiteralLength", "zstd", "lz4")
    lateinit var codecName: String

    @Param("commitMetadata", "clustered", "sparse", "dense", "random")
    lateinit var pattern: String

    @Param("131072")
    var bitCount: Int = 0

    @Param("1", "2", "3", "42", "99")
    var seed: Int = 0

    private lateinit var codec: OffsetMetadataCodec
    private lateinit var snapshot: OffsetBitsetSnapshot
    private lateinit var encoded: ByteArray

    @Setup(Level.Trial)
    fun setup() {
        codec = offsetMetadataCodecs().single { it.name == codecName }
        snapshot = buildSnapshot(pattern, bitCount, seed)
        encoded = codec.encode(snapshot)
    }

    @Benchmark
    fun encode(blackhole: Blackhole) {
        blackhole.consume(codec.encode(snapshot))
    }

    @Benchmark
    fun decode(blackhole: Blackhole) {
        blackhole.consume(codec.decode(encoded))
    }

    private fun buildSnapshot(pattern: String, bitCount: Int, seed: Int): OffsetBitsetSnapshot {
        if (pattern == "commitMetadata") {
            require(bitCount == 16 * 1024 * 8) { "commitMetadata uses exactly 16 KiB of bitset bytes" }
            return commitMetadataExperimentSnapshot(seed)
        }

        val words = LongArray(wordsForBits(bitCount))
        val random = kotlin.random.Random(seed)
        for (bitIndex in 0 until bitCount) {
            val processed = when (pattern) {
                "clustered" -> bitIndex in 0 until bitCount / 3 || bitIndex in (bitCount * 2 / 3) until (bitCount * 2 / 3 + bitCount / 16)
                "sparse" -> bitIndex % 97 == 0
                "dense" -> bitIndex % 97 != 0
                "random" -> random.nextInt(100) < 50
                else -> error("Unknown pattern: $pattern")
            }
            if (processed) {
                words[bitIndex ushr 6] = words[bitIndex ushr 6] or (1L shl bitIndex)
            }
        }
        return OffsetBitsetSnapshot(baseOffset = 1_000_000L, bitCount = bitCount, words = words)
    }
}
