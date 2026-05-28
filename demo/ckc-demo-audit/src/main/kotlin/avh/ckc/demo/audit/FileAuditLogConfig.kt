package avh.ckc.demo.audit

import java.nio.file.Path
import kotlin.io.path.Path

data class FileAuditLogConfig(
    val directory: Path,
    val filePrefix: String,
    val queueCapacity: Int = 65_536,
    val flushRecords: Int = 1_024,
    val flushIntervalMs: Long = 50,
    val fsyncIntervalMs: Long = 1_000,
    val maxSegmentBytes: Long = 256L * 1024L * 1024L
) {
    init {
        require(filePrefix.isNotBlank()) { "filePrefix must not be blank" }
        require(queueCapacity > 0) { "queueCapacity must be positive" }
        require(flushRecords > 0) { "flushRecords must be positive" }
        require(flushIntervalMs > 0) { "flushIntervalMs must be positive" }
        require(fsyncIntervalMs >= 0) { "fsyncIntervalMs must be non-negative" }
        require(maxSegmentBytes > 0) { "maxSegmentBytes must be positive" }
    }

    companion object {
        fun fromEnvironment(
            environment: Map<String, String> = System.getenv(),
            defaultDirectory: String = ".demo-audit",
            defaultFilePrefix: String = "audit"
        ): FileAuditLogConfig =
            FileAuditLogConfig(
                directory = Path(environment["AUDIT_LOG_DIR"] ?: defaultDirectory),
                filePrefix = environment["AUDIT_LOG_FILE_PREFIX"] ?: defaultFilePrefix,
                queueCapacity = environment["AUDIT_QUEUE_CAPACITY"]?.toIntOrNull() ?: 65_536,
                flushRecords = environment["AUDIT_FLUSH_RECORDS"]?.toIntOrNull() ?: 1_024,
                flushIntervalMs = environment["AUDIT_FLUSH_INTERVAL_MS"]?.toLongOrNull() ?: 50,
                fsyncIntervalMs = environment["AUDIT_FSYNC_INTERVAL_MS"]?.toLongOrNull() ?: 1_000,
                maxSegmentBytes = environment["AUDIT_MAX_SEGMENT_BYTES"]?.toLongOrNull() ?: 256L * 1024L * 1024L
            )
    }
}
