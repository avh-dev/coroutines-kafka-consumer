package avh.ckc.loadtest.metrics

import avh.ckc.loadtest.kafka.ProducerPoolSizes
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import io.micrometer.core.instrument.FunctionCounter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.apache.kafka.clients.producer.Producer
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

class LoadTestMetrics(
    port: Int,
    shardIndex: Int,
    poolSizes: ProducerPoolSizes
) : AutoCloseable {
    val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
    private val shardTag = Tag.of("shard", shardIndex.toString())
    private val kafkaMetricsScheduler = Executors.newSingleThreadScheduledExecutor { runnable ->
        Thread(runnable, "load-test-kafka-metrics").apply { isDaemon = true }
    }
    private val kafkaBinders = CopyOnWriteArrayList<KafkaClientMetrics>()
    private val server = HttpServer.create(InetSocketAddress(port), 0).apply {
        createContext("/metrics", ::serveMetrics)
        executor = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "load-test-metrics-http").apply { isDaemon = true }
        }
        start()
    }

    init {
        registerPoolSize("order", poolSizes.order)
        registerPoolSize("batch", poolSizes.batch)
        registerPoolSize("telemetry", poolSizes.cauldronTelemetry)
    }

    fun registerTopicStats(topic: String, stats: ProducerTopicStats) {
        registerCounter("ckc.load.test.producer.records.sent", "Records offered to the producer", topic, stats.sent)
        registerCounter("ckc.load.test.producer.records.acked", "Records acknowledged by Kafka", topic, stats.acked)
        registerCounter("ckc.load.test.producer.records.failed", "Records rejected or failed by Kafka", topic, stats.failed)
    }

    fun bindKafkaProducer(topic: String, producerIndex: Int, producer: Producer<*, *>) {
        KafkaClientMetrics(
            producer,
            listOf(shardTag, Tag.of("traffic.topic", topic), Tag.of("producer.index", producerIndex.toString())),
            kafkaMetricsScheduler
        ).also { binder ->
            binder.bindTo(registry)
            kafkaBinders += binder
        }
    }

    override fun close() {
        server.stop(0)
        (server.executor as? java.util.concurrent.ExecutorService)?.shutdownNow()
        kafkaBinders.forEach(KafkaClientMetrics::close)
        kafkaMetricsScheduler.shutdownNow()
        registry.close()
    }

    private fun registerPoolSize(topic: String, size: Int) {
        Gauge.builder("ckc.load.test.producer.pool.size", size) { it.toDouble() }
            .description("Configured Kafka producer instances for a generated topic")
            .tags(listOf(shardTag, Tag.of("traffic.topic", topic)))
            .register(registry)
    }

    private fun registerCounter(name: String, description: String, topic: String, value: AtomicLong) {
        FunctionCounter.builder(name, value) { it.get().toDouble() }
            .description(description)
            .tags(listOf(shardTag, Tag.of("traffic.topic", topic)))
            .register(registry)
    }

    private fun serveMetrics(exchange: HttpExchange) {
        if (exchange.requestMethod != "GET") {
            exchange.sendResponseHeaders(405, -1)
            exchange.close()
            return
        }
        val body = registry.scrape().toByteArray(StandardCharsets.UTF_8)
        exchange.responseHeaders.set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        exchange.sendResponseHeaders(200, body.size.toLong())
        exchange.responseBody.use { it.write(body) }
    }
}

class ProducerTopicStats {
    val sent = AtomicLong()
    val acked = AtomicLong()
    val failed = AtomicLong()
}
