package avh.ckc.loadtest.kafka

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.serialization.BatchLifecycleEventSerializer
import avh.ckc.demo.serialization.CauldronTelemetryEventSerializer
import avh.ckc.demo.serialization.OrderLifecycleEventSerializer
import avh.ckc.loadtest.config.LoadTestConfig
import avh.ckc.loadtest.metrics.LoadTestMetrics
import avh.ckc.loadtest.metrics.ProducerTopicStats
import avh.ckc.loadtest.runtime.ShardContext
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.serialization.Serializer
import java.lang.Math.floorMod
import java.util.concurrent.atomic.AtomicLong

interface LoadTestPublisher {
    fun sendOrder(key: String, event: OrderLifecycleEvent)

    fun sendBatch(key: String, event: BatchLifecycleEvent)

    fun sendTelemetry(key: String, event: CauldronTelemetryEvent)

    fun flush()

    fun logSnapshot(reason: String)
}

class LoadTestProducers(
    private val config: LoadTestConfig,
    private val shardContext: ShardContext,
    private val poolSizes: ProducerPoolSizes,
    private val metrics: LoadTestMetrics,
    private val auditLog: LoadTestAuditLog? = if (config.auditLogEnabled) LoadTestAuditLog.fromConfig(config, shardContext) else null
) : LoadTestPublisher, AutoCloseable {
    private val lifecycleStats = ProducerTopicStats()
    private val batchStats = ProducerTopicStats()
    private val telemetryStats = ProducerTopicStats()
    private val lastLoggedTotal = AtomicLong(0)

    private val lifecycleProducerPool = lazy {
        producerPool("order", poolSizes.order, OrderLifecycleEventSerializer::class.java)
    }
    private val batchProducerPool = lazy {
        producerPool("batch", poolSizes.batch, BatchLifecycleEventSerializer::class.java)
    }
    private val telemetryProducerPool = lazy {
        producerPool("telemetry", poolSizes.cauldronTelemetry, CauldronTelemetryEventSerializer::class.java)
    }

    init {
        metrics.registerTopicStats("order", lifecycleStats)
        metrics.registerTopicStats("batch", batchStats)
        metrics.registerTopicStats("telemetry", telemetryStats)
    }

    override fun sendOrder(key: String, event: OrderLifecycleEvent) {
        val sent = lifecycleStats.sent.incrementAndGet()
        if (!config.publishEnabled) {
            recordDryRun(config.orderEventsTopic, key, lifecycleStats.acked)
            maybeLogProgress(sent + batchStats.sent.get() + telemetryStats.sent.get())
            return
        }
        lifecycleProducerPool.value.send(
            ProducerRecord(config.orderEventsTopic, key, event),
            callback(
                stream = "order",
                key = key,
                ackedCounter = lifecycleStats.acked,
                failureCounter = lifecycleStats.failed
            )
        )
        maybeLogProgress(sent + batchStats.sent.get() + telemetryStats.sent.get())
    }

    override fun sendBatch(key: String, event: BatchLifecycleEvent) {
        val sent = batchStats.sent.incrementAndGet()
        if (!config.publishEnabled) {
            recordDryRun(config.batchEventsTopic, key, batchStats.acked)
            maybeLogProgress(lifecycleStats.sent.get() + sent + telemetryStats.sent.get())
            return
        }
        batchProducerPool.value.send(
            ProducerRecord(config.batchEventsTopic, key, event),
            callback(
                stream = "batch",
                key = key,
                ackedCounter = batchStats.acked,
                failureCounter = batchStats.failed
            )
        )
        maybeLogProgress(lifecycleStats.sent.get() + sent + telemetryStats.sent.get())
    }

    override fun sendTelemetry(key: String, event: CauldronTelemetryEvent) {
        val sent = telemetryStats.sent.incrementAndGet()
        if (!config.publishEnabled) {
            recordDryRun(config.cauldronEventsTopic, key, telemetryStats.acked)
            maybeLogProgress(lifecycleStats.sent.get() + batchStats.sent.get() + sent)
            return
        }
        telemetryProducerPool.value.send(
            ProducerRecord(config.cauldronEventsTopic, key, event),
            callback(
                stream = "telemetry",
                key = key,
                ackedCounter = telemetryStats.acked,
                failureCounter = telemetryStats.failed
            )
        )
        maybeLogProgress(lifecycleStats.sent.get() + batchStats.sent.get() + sent)
    }

    override fun flush() {
        if (config.publishEnabled) {
            initializedPools().forEach(TopicProducerPool<*, *>::flush)
        }
        logSnapshot("flush")
    }

    override fun logSnapshot(reason: String) {
        println(
            "load-test producer snapshot reason=$reason " +
                "order(pool=${poolSizes.order},sent=${lifecycleStats.sent.get()},acked=${lifecycleStats.acked.get()},failed=${lifecycleStats.failed.get()}) " +
                "batch(pool=${poolSizes.batch},sent=${batchStats.sent.get()},acked=${batchStats.acked.get()},failed=${batchStats.failed.get()}) " +
                "telemetry(pool=${poolSizes.cauldronTelemetry},sent=${telemetryStats.sent.get()},acked=${telemetryStats.acked.get()},failed=${telemetryStats.failed.get()})"
        )
    }

    override fun close() {
        try {
            flush()
        } finally {
            if (config.publishEnabled) {
                initializedPools().forEach(TopicProducerPool<*, *>::close)
            }
            logSnapshot("close")
        }
    }

    private fun recordDryRun(topic: String, key: String, ackedCounter: AtomicLong) {
        ackedCounter.incrementAndGet()
        auditLog?.generated(topic, key)
    }

    private fun producerProperties(valueSerializerClass: Class<*>): Map<String, Any> = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to config.bootstrapServers,
        ProducerConfig.ACKS_CONFIG to "all",
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
        ProducerConfig.LINGER_MS_CONFIG to config.kafkaProducer.lingerMs,
        ProducerConfig.BATCH_SIZE_CONFIG to config.kafkaProducer.batchSize,
        ProducerConfig.COMPRESSION_TYPE_CONFIG to config.kafkaProducer.compressionType,
        ProducerConfig.BUFFER_MEMORY_CONFIG to config.kafkaProducer.bufferMemory,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to valueSerializerClass
    )

    private fun callback(
        stream: String,
        key: String,
        ackedCounter: AtomicLong,
        failureCounter: AtomicLong
    ): Callback =
        Callback { metadata: RecordMetadata?, exception: Exception? ->
            if (exception != null) {
                failureCounter.incrementAndGet()
                println(
                    "load-test publish failed stream=$stream key=$key topic=${topicName(stream)} error=${exception::class.java.simpleName}: ${exception.message}"
                )
                return@Callback
            }

            ackedCounter.incrementAndGet()
            val recordMetadata = metadata!!
            auditLog?.published(recordMetadata, key)
        }

    private fun maybeLogProgress(totalSent: Long) {
        val previous = lastLoggedTotal.get()
        if (totalSent >= 1_000 && totalSent / 1_000 > previous / 1_000) {
            if (lastLoggedTotal.compareAndSet(previous, totalSent)) {
                logSnapshot("progress-$totalSent")
            }
        }
    }

    private fun topicName(stream: String): String =
        when (stream) {
            "order" -> config.orderEventsTopic
            "batch" -> config.batchEventsTopic
            "telemetry" -> config.cauldronEventsTopic
            else -> "unknown"
        }

    private fun <V> producerPool(
        topic: String,
        size: Int,
        valueSerializerClass: Class<out Serializer<V>>
    ): TopicProducerPool<String, V> =
        TopicProducerPool(
            List(size) { producerIndex ->
                KafkaProducer<String, V>(
                    producerProperties(valueSerializerClass) +
                        (ProducerConfig.CLIENT_ID_CONFIG to "ckc-load-test-$topic-s${shardContext.shardIndex}-p$producerIndex")
                ).also { producer -> metrics.bindKafkaProducer(topic, producerIndex, producer) }
            }
        )

    private fun initializedPools(): List<TopicProducerPool<*, *>> = buildList {
        if (lifecycleProducerPool.isInitialized()) add(lifecycleProducerPool.value)
        if (batchProducerPool.isInitialized()) add(batchProducerPool.value)
        if (telemetryProducerPool.isInitialized()) add(telemetryProducerPool.value)
    }
}

internal class TopicProducerPool<K, V>(
    private val producers: List<Producer<K, V>>
) {
    init {
        require(producers.isNotEmpty()) { "producer pool must not be empty" }
    }

    fun send(record: ProducerRecord<K, V>, callback: Callback) =
        producers[indexFor(record.key())].send(record, callback)

    fun flush() = producers.forEach(Producer<K, V>::flush)

    fun close() = producers.forEach(Producer<K, V>::close)

    internal fun indexFor(key: K?): Int = floorMod(key?.hashCode() ?: 0, producers.size)
}
