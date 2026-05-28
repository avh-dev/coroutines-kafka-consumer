package avh.ckc.demo.audit

import java.io.BufferedOutputStream
import java.io.Closeable
import java.io.FileOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.io.path.exists

class FileBackedAuditLog(
    private val config: FileAuditLogConfig,
    private val topicIds: AuditTopicIds = AuditTopicIds.DemoDefaults,
    threadName: String = "ckc-demo-audit-writer"
) : Closeable {
    private val queue = ArrayBlockingQueue<AuditRecord>(config.queueCapacity)
    private val closed = AtomicBoolean(false)
    private val writer = AuditWriter(config, topicIds, queue, closed)
    private val writerThread = Thread(writer, threadName).apply {
        isDaemon = false
        start()
    }

    fun published(topic: String, key: String, partition: Int, offset: Long, kafkaTimestampMs: Long) {
        append(AuditEventType.PUBLISHED, topic, key, partition, offset, kafkaTimestampMs)
    }

    fun processed(topic: String, key: String, partition: Int, offset: Long, kafkaTimestampMs: Long) {
        append(AuditEventType.PROCESSED, topic, key, partition, offset, kafkaTimestampMs)
    }

    fun append(type: AuditEventType, topic: String, key: String, partition: Int, offset: Long, kafkaTimestampMs: Long) {
        check(!closed.get()) { "Audit log is closed" }
        val record = AuditRecord(
            type = type,
            topic = topic,
            key = key,
            partition = partition,
            offset = offset,
            kafkaTimestampMs = kafkaTimestampMs,
            auditTimestampMs = System.currentTimeMillis()
        )
        try {
            queue.put(record)
        } catch (error: InterruptedException) {
            Thread.currentThread().interrupt()
            throw IllegalStateException("Interrupted while enqueueing audit record", error)
        }
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            try {
                writerThread.join()
            } catch (error: InterruptedException) {
                Thread.currentThread().interrupt()
                throw IllegalStateException("Interrupted while closing audit log", error)
            }
        }
        writer.failure?.let { throw it }
    }
}

private class AuditWriter(
    private val config: FileAuditLogConfig,
    private val topicIds: AuditTopicIds,
    private val queue: ArrayBlockingQueue<AuditRecord>,
    private val closed: AtomicBoolean
) : Runnable {
    @Volatile
    var failure: RuntimeException? = null
        private set

    private val batch = ArrayList<AuditRecord>(config.flushRecords)
    private var segmentIndex = 0
    private var currentPath: Path? = null
    private var currentFileOutput: FileOutputStream? = null
    private var output: BufferedOutputStream? = null
    private var currentSegmentBytes = 0L
    private var lastFlushMs = System.currentTimeMillis()
    private var lastFsyncMs = lastFlushMs

    override fun run() {
        try {
            Files.createDirectories(config.directory)
            while (!closed.get() || queue.isNotEmpty()) {
                val first = queue.poll(config.flushIntervalMs, TimeUnit.MILLISECONDS)
                if (first != null) {
                    batch.add(first)
                    queue.drainTo(batch, config.flushRecords - 1)
                    writeBatch(batch)
                    batch.clear()
                }
                flushIfDue()
            }
            flush(forceFsync = true)
        } catch (error: InterruptedException) {
            Thread.currentThread().interrupt()
            failure = IllegalStateException("Audit writer was interrupted before draining all records", error)
        } catch (error: RuntimeException) {
            failure = error
        } finally {
            output?.close()
            currentFileOutput?.close()
        }
    }

    private fun writeBatch(records: List<AuditRecord>) {
        val builder = StringBuilder(records.size * 80)
        records.forEach { appendTsv(builder, it) }
        val bytes = builder.toString().toByteArray(StandardCharsets.UTF_8)
        if (currentSegmentBytes > 0 && currentSegmentBytes + bytes.size > config.maxSegmentBytes) {
            flush(forceFsync = true)
            output?.close()
            currentFileOutput?.close()
            output = null
            currentFileOutput = null
            currentPath = null
            currentSegmentBytes = 0
        }
        stream().write(bytes)
        currentSegmentBytes += bytes.size
        if (records.size >= config.flushRecords) {
            flushIfDue(force = true)
        }
    }

    private fun appendTsv(builder: StringBuilder, record: AuditRecord) {
        builder
            .append(record.type.code)
            .append('\t')
            .append(topicIds.idOf(record.topic))
            .append('\t')
            .append(record.partition)
            .append('\t')
            .append(record.offset)
            .append('\t')
            .append(record.kafkaTimestampMs)
            .append('\t')
            .append(record.auditTimestampMs)
            .append('\t')
        appendEscapedKey(builder, record.key)
        builder.append('\n')
    }

    private fun appendEscapedKey(builder: StringBuilder, key: String) {
        key.forEach { char ->
            builder.append(
                when (char) {
                    '\t', '\r', '\n' -> ' '
                    else -> char
                }
            )
        }
    }

    private fun stream(): BufferedOutputStream {
        output?.let { return it }
        val path = nextSegmentPath()
        currentPath = path
        currentFileOutput = FileOutputStream(path.toFile(), true)
        output = BufferedOutputStream(currentFileOutput, 64 * 1024)
        return output ?: error("Audit output stream was not initialized")
    }

    private fun nextSegmentPath(): Path {
        while (true) {
            val candidate = config.directory.resolve("${config.filePrefix}-${segmentIndex.toString().padStart(6, '0')}.tsv")
            segmentIndex += 1
            if (!candidate.exists()) {
                Files.writeString(
                    candidate,
                    "# ckc-demo-audit-v1\n",
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE
                )
                currentSegmentBytes = Files.size(candidate)
                return candidate
            }
        }
    }

    private fun flushIfDue(force: Boolean = false) {
        val now = System.currentTimeMillis()
        if (force || now - lastFlushMs >= config.flushIntervalMs) {
            flush(forceFsync = config.fsyncIntervalMs > 0 && now - lastFsyncMs >= config.fsyncIntervalMs)
        }
    }

    private fun flush(forceFsync: Boolean) {
        val stream = output ?: return
        stream.flush()
        lastFlushMs = System.currentTimeMillis()
        if (forceFsync) {
            currentFileOutput?.channel?.force(true)
            lastFsyncMs = lastFlushMs
        }
    }
}
