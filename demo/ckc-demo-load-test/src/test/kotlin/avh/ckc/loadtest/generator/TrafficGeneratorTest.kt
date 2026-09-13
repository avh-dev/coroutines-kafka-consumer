package avh.ckc.loadtest.generator

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.loadtest.config.LoadTestConfig
import avh.ckc.loadtest.config.TelemetrySourceMode
import avh.ckc.loadtest.kafka.LoadTestPublisher
import avh.ckc.loadtest.runtime.ShardContext
import avh.ckc.loadtest.scenario.LoadScenario
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.time.Duration
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertTrue

class TrafficGeneratorTest {
    @Test
    fun `stops after load profile ends without draining generated domain state`() = runBlocking {
        val publisher = RecordingPublisher()
        val config = LoadTestConfig(
            bootstrapServers = "localhost:9092",
            orderEventsTopic = "order.events.v1",
            batchEventsTopic = "batch.events.v1",
            cauldronEventsTopic = "cauldron.events.v1",
            baseTps = 300,
            orderEventPercent = 35,
            batchEventPercent = 35,
            cauldronTelemetryPercent = 30,
            loadProfile = "100 -> (1s, steady) -> 100",
            cauldronCount = 8,
            minOrdersPerBatch = 3,
            maxOrdersPerBatch = 5,
            minBrewingSteps = 5,
            maxBrewingSteps = 8,
            brewingStepBurstEvery = 10,
            minBrewingStepBurst = 2,
            maxBrewingStepBurst = 5,
            maxBurst = 100,
            statsLogInterval = Duration.ofSeconds(30),
            diagnosticsBlobSize = 8,
            telemetrySourceMode = TelemetrySourceMode.ACTIVE_BATCHES,
            publishEnabled = true,
            auditLogEnabled = false
        )

        withTimeout(2_500) {
            TrafficGenerator(
                shardContext = ShardContext(shardIndex = 0, totalShards = 1, testRunId = "test", testRunStartedAt = null),
                config = config,
                scenario = LoadScenario.parse(config.loadProfile),
                producers = publisher
            ).run()
        }

        assertTrue(publisher.orderSent > 0)
        assertTrue(publisher.batchSent > 0)
        assertTrue(publisher.telemetrySent > 0)
        assertTrue(publisher.flushed)
    }

    @Test
    fun `delegates active telemetry warmup when no active cauldron exists`() = runBlocking {
        val publisher = RecordingPublisher()
        val config = LoadTestConfig(
            bootstrapServers = "localhost:9092",
            orderEventsTopic = "order.events.v1",
            batchEventsTopic = "batch.events.v1",
            cauldronEventsTopic = "cauldron.events.v1",
            baseTps = 100,
            orderEventPercent = 0,
            batchEventPercent = 0,
            cauldronTelemetryPercent = 100,
            loadProfile = "100 -> (1s, steady) -> 100",
            cauldronCount = 1,
            minOrdersPerBatch = 3,
            maxOrdersPerBatch = 3,
            minBrewingSteps = 5,
            maxBrewingSteps = 5,
            brewingStepBurstEvery = 10,
            minBrewingStepBurst = 2,
            maxBrewingStepBurst = 5,
            maxBurst = 100,
            statsLogInterval = Duration.ofSeconds(30),
            diagnosticsBlobSize = 8,
            telemetrySourceMode = TelemetrySourceMode.ACTIVE_BATCHES,
            publishEnabled = true,
            auditLogEnabled = false
        )

        withTimeout(2_500) {
            TrafficGenerator(
                shardContext = ShardContext(shardIndex = 0, totalShards = 1, testRunId = "test", testRunStartedAt = null),
                config = config,
                scenario = LoadScenario.parse(config.loadProfile),
                producers = publisher
            ).run()
        }

        assertTrue(publisher.telemetryKeys.isNotEmpty())
        assertTrue(publisher.batchSent > 0)
        assertTrue(publisher.orderSent > 0)
        assertTrue(publisher.telemetryKeys.all { it.startsWith("cauldron-") })
        assertTrue(publisher.totalSent <= 105, "Delegated publications exceeded the 100 msg/s budget: ${publisher.totalSent}")
    }

    @Test
    fun `fleet telemetry cycles isolated cauldron keys at a fixed cadence`() = runBlocking {
        val publisher = RecordingPublisher(firstTelemetryDelay = Duration.ofMillis(60))
        val config = LoadTestConfig(
            bootstrapServers = "localhost:9092",
            orderEventsTopic = "order.events.v1",
            batchEventsTopic = "batch.events.v1",
            cauldronEventsTopic = "cauldron.events.v1",
            baseTps = 10,
            orderEventPercent = 0,
            batchEventPercent = 0,
            cauldronTelemetryPercent = 100,
            loadProfile = "100 -> (1s, steady) -> 100",
            cauldronCount = 4,
            minOrdersPerBatch = 3,
            maxOrdersPerBatch = 3,
            minBrewingSteps = 5,
            maxBrewingSteps = 5,
            brewingStepBurstEvery = 10,
            minBrewingStepBurst = 2,
            maxBrewingStepBurst = 5,
            maxBurst = 100,
            statsLogInterval = Duration.ofSeconds(30),
            diagnosticsBlobSize = 8,
            telemetrySourceMode = TelemetrySourceMode.FLEET,
            telemetryPublishInterval = Duration.ofMillis(100),
            publishEnabled = true,
            auditLogEnabled = false
        )

        withTimeout(2_500) {
            TrafficGenerator(
                shardContext = ShardContext(shardIndex = 0, totalShards = 1, testRunId = "test", testRunStartedAt = null),
                config = config,
                scenario = LoadScenario.parse(config.loadProfile),
                producers = publisher
            ).run()
        }

        assertTrue(publisher.telemetryKeys.isNotEmpty())
        assertTrue(publisher.batchSent == 0)
        assertTrue(publisher.telemetryKeys.all { it.startsWith("fleet-cauldron-0-0-") })
        val gaps = publisher.telemetrySentAtNanos.zipWithNext { first, second -> second - first }
        assertTrue(gaps.size >= 2)
        assertTrue(
            gaps.all { it >= Duration.ofMillis(95).toNanos() },
            "Telemetry cadence was shortened after a blocking send: $gaps"
        )
    }

    private class RecordingPublisher(
        private val firstTelemetryDelay: Duration = Duration.ZERO
    ) : LoadTestPublisher {
        private val orderCounter = AtomicInteger()
        private val batchCounter = AtomicInteger()
        private val telemetryCounter = AtomicInteger()
        private val flushedFlag = AtomicBoolean(false)
        val telemetryKeys: MutableList<String> = Collections.synchronizedList(mutableListOf())
        val telemetrySentAtNanos: MutableList<Long> = Collections.synchronizedList(mutableListOf())

        val orderSent: Int
            get() = orderCounter.get()
        val batchSent: Int
            get() = batchCounter.get()
        val telemetrySent: Int
            get() = telemetryCounter.get()
        val totalSent: Int
            get() = orderSent + batchSent + telemetrySent
        val flushed: Boolean
            get() = flushedFlag.get()

        override fun sendOrder(key: String, event: OrderLifecycleEvent) {
            orderCounter.incrementAndGet()
        }

        override fun sendBatch(key: String, event: BatchLifecycleEvent) {
            batchCounter.incrementAndGet()
        }

        override fun sendTelemetry(key: String, event: CauldronTelemetryEvent) {
            if (telemetryCounter.get() == 0 && !firstTelemetryDelay.isZero) {
                Thread.sleep(firstTelemetryDelay.toMillis())
            }
            telemetryKeys += key
            telemetrySentAtNanos += System.nanoTime()
            telemetryCounter.incrementAndGet()
        }

        override fun flush() {
            flushedFlag.set(true)
        }

        override fun logSnapshot(reason: String) = Unit
    }
}
