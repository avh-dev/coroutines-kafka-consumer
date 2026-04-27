package avh.ckc.loadtest.kafka

import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.serialization.CauldronTelemetryEventSerializer
import avh.ckc.demo.serialization.OrderLifecycleEventSerializer
import avh.ckc.loadtest.config.LoadTestConfig
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.util.concurrent.atomic.AtomicLong

class LoadTestProducers(
    private val config: LoadTestConfig
) : AutoCloseable {
    private val lifecycleSent = AtomicLong(0)
    private val lifecycleAcked = AtomicLong(0)
    private val lifecycleFailed = AtomicLong(0)
    private val telemetrySent = AtomicLong(0)
    private val telemetryAcked = AtomicLong(0)
    private val telemetryFailed = AtomicLong(0)
    private val lastLoggedTotal = AtomicLong(0)

    private val lifecycleProducer = KafkaProducer<String, OrderLifecycleEvent>(
        producerProperties(OrderLifecycleEventSerializer::class.java)
    )
    private val telemetryProducer = KafkaProducer<String, CauldronTelemetryEvent>(
        producerProperties(CauldronTelemetryEventSerializer::class.java)
    )

    fun sendLifecycle(key: String, event: OrderLifecycleEvent) {
        val sent = lifecycleSent.incrementAndGet()
        lifecycleProducer.send(
            ProducerRecord(config.orderLifecycleTopic, key, event),
            callback(stream = "lifecycle", key = key, sentCounter = lifecycleAcked, failureCounter = lifecycleFailed)
        )
        maybeLogProgress(sent + telemetrySent.get())
    }

    fun sendTelemetry(key: String, event: CauldronTelemetryEvent) {
        val sent = telemetrySent.incrementAndGet()
        telemetryProducer.send(
            ProducerRecord(config.cauldronTelemetryTopic, key, event),
            callback(stream = "telemetry", key = key, sentCounter = telemetryAcked, failureCounter = telemetryFailed)
        )
        maybeLogProgress(lifecycleSent.get() + sent)
    }

    fun flush() {
        lifecycleProducer.flush()
        telemetryProducer.flush()
        logSnapshot("flush")
    }

    fun logSnapshot(reason: String) {
        println(
            "load-test producer snapshot reason=$reason " +
                "lifecycle(sent=${lifecycleSent.get()}, acked=${lifecycleAcked.get()}, failed=${lifecycleFailed.get()}) " +
                "telemetry(sent=${telemetrySent.get()}, acked=${telemetryAcked.get()}, failed=${telemetryFailed.get()})"
        )
    }

    override fun close() {
        try {
            flush()
        } finally {
            lifecycleProducer.close()
            telemetryProducer.close()
            logSnapshot("close")
        }
    }

    private fun producerProperties(valueSerializerClass: Class<*>): Map<String, Any> = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to config.bootstrapServers,
        ProducerConfig.ACKS_CONFIG to "all",
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
        ProducerConfig.LINGER_MS_CONFIG to 20,
        ProducerConfig.BATCH_SIZE_CONFIG to 64 * 1024,
        ProducerConfig.COMPRESSION_TYPE_CONFIG to "lz4",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to valueSerializerClass
    )

    private fun callback(
        stream: String,
        key: String,
        sentCounter: AtomicLong,
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

            val acked = sentCounter.incrementAndGet()
            if (acked <= 5 || acked % 500 == 0L) {
                println(
                    "load-test publish ack stream=$stream key=$key " +
                        "topic=${metadata!!.topic()} partition=${metadata.partition()} offset=${metadata.offset()}"
                )
            }
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
            "lifecycle" -> config.orderLifecycleTopic
            "telemetry" -> config.cauldronTelemetryTopic
            else -> "unknown"
        }
}
