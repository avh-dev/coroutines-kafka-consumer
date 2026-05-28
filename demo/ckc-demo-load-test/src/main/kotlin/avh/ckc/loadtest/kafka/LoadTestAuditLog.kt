package avh.ckc.loadtest.kafka

import avh.ckc.demo.audit.FileAuditLogConfig
import avh.ckc.demo.audit.FileBackedAuditLog
import org.apache.kafka.clients.producer.RecordMetadata

class LoadTestAuditLog(
    private val auditLog: FileBackedAuditLog
) : AutoCloseable {
    fun published(metadata: RecordMetadata, key: String, eventType: String) {
        auditLog.published(
            topic = metadata.topic(),
            key = key,
            partition = metadata.partition(),
            offset = metadata.offset(),
            kafkaTimestampMs = metadata.timestamp()
        )
    }

    fun generated(topic: String, key: String, eventType: String) {
        auditLog.published(
            topic = topic,
            key = key,
            partition = -1,
            offset = -1,
            kafkaTimestampMs = System.currentTimeMillis()
        )
    }

    override fun close() {
        auditLog.close()
    }

    companion object {
        fun fromEnvironment(environment: Map<String, String> = System.getenv()): LoadTestAuditLog =
            LoadTestAuditLog(
                FileBackedAuditLog(
                    FileAuditLogConfig.fromEnvironment(
                        environment = environment,
                        defaultFilePrefix = "published"
                    ),
                    threadName = "ckc-load-test-audit-writer"
                )
            )
    }
}
