package avh.ckc.loadtest.kafka

import avh.ckc.demo.audit.AuditLineWriter
import avh.ckc.demo.audit.TcpAuditClient
import avh.ckc.demo.audit.encodeAuditRecord
import avh.ckc.demo.audit.sanitizeAuditComponent
import avh.ckc.loadtest.config.LoadTestConfig
import avh.ckc.loadtest.runtime.ShardContext
import org.apache.kafka.clients.producer.RecordMetadata

class LoadTestAuditLog private constructor(
    private val writer: AuditLineWriter,
    private val runId: String,
    private val writerId: String
) : AutoCloseable {
    fun published(metadata: RecordMetadata, key: String) {
        append(
            encodeAuditRecord(
                type = "P",
                runId = runId,
                writerId = writerId,
                topic = metadata.topic(),
                partition = metadata.partition(),
                offset = metadata.offset(),
                kafkaTimestampMs = metadata.timestamp(),
                messageKey = key
            )
        )
    }

    fun generated(topic: String, key: String) {
        append(
            encodeAuditRecord(
                type = "P",
                runId = runId,
                writerId = writerId,
                topic = topic,
                partition = -1,
                offset = -1,
                kafkaTimestampMs = System.currentTimeMillis(),
                messageKey = key
            )
        )
    }

    override fun close() {
        writer.close()
    }

    private fun append(record: String) {
        writer.write(record)
    }

    companion object {
        fun fromConfig(config: LoadTestConfig, shardContext: ShardContext): LoadTestAuditLog =
            LoadTestAuditLog(
                writer = TcpAuditClient(config.auditHost, config.auditPort),
                runId = config.auditRunId,
                writerId = writerId(shardContext)
            )
    }
}

internal fun writerId(shardContext: ShardContext): String =
    sanitizeAuditComponent("loadtest-shard-${shardContext.shardIndex}", "loadtest")
