# Experiments

This module contains historical implementations and benchmarks
used to evaluate and select the final `OffsetTracker`.

There is intentionally **no production code** in this module.
All implementations live in test fixtures and are used only by
tests and JMH benchmarks.

The module is excluded from the build by default.

To execute test or run benchmarks use parameter `-PwithExperiments=true`

## Offset metadata compression

`OffsetMetadataCompressionBenchmark` compares candidate encodings for a future
commit metadata payload that can preserve processed but not yet committed
offsets across restarts.

Current candidates:

- `raw` stores the normalized `LongArray` bitset directly.
- `wordRle` stores repeated 64-bit words as run-length/value pairs.
- `bitRle` stores runs of individual processed/unprocessed bits.
- `byteRle` scans little-endian bitset bytes once, keeps literal blocks as-is,
  and emits runs only for repeated `0x00` or `0xff` bytes. Literal and run
  blocks always alternate; adjacent `0x00`/`0xff` runs are separated by an
  empty literal block. The default minimum run is three bytes.
- `byteRleNoLiteralLength` writes non-special bytes directly and always writes
  `0x00`/`0xff` as run markers with lengths, even for single-byte runs.
- `zstd` stores the same header as the custom codecs and compresses the bitset
  byte payload with zstd-jni.
- `lz4` stores the same header as the custom codecs and compresses the bitset
  byte payload with lz4-java's fastest compressor.

The benchmark measures encode and decode throughput. `OffsetMetadataCompressionReport`
prints exact binary payload bytes before any Kafka metadata string encoding. If
the final core design stores the payload in Kafka's string metadata field,
base64 or another text-safe wrapper must be measured separately.

The default `commitMetadata` benchmark profile uses a 16 KiB bitset byte
payload and can be run across multiple fixed seeds:

- first 15 KiB: about 98% processed bits and random gaps;
- next 824 bytes: about 20% processed bits;
- final 200 bytes: about 50% processed bits.

Run the compression benchmark:

```shell
./gradlew :ckc-experiments:jmh -PwithExperiments=true
```

Build the JMH jar and print the payload-size report:

```shell
./gradlew :ckc-experiments:jmhJar -PwithExperiments=true
java -cp "experiments/ckc-experiments/build/libs/*" avh.ckc.core.offset.compression.OffsetMetadataCompressionReportKt
```

Use a Java 21 runtime for direct JMH jar execution.

Run all experiment tests:

```shell
./gradlew :ckc-experiments:test -PwithExperiments=true
```
