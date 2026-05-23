package avh.ckc.loadtest.generator

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.loadtest.config.LoadTestConfig
import avh.ckc.loadtest.kafka.LoadTestPublisher
import avh.ckc.loadtest.runtime.ShardContext
import avh.ckc.loadtest.scenario.LoadScenario
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.time.Duration
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
            lifecycleBaseRate = 5,
            telemetryBaseRate = 10,
            loadProfile = "100 -> (1s, steady) -> 100",
            lifecycleOrdersPerBatch = 3,
            telemetryInterval = Duration.ofSeconds(10),
            tickInterval = Duration.ofMillis(100),
            diagnosticsBlobSize = 8,
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

    private class RecordingPublisher : LoadTestPublisher {
        var orderSent = 0
            private set
        var batchSent = 0
            private set
        var telemetrySent = 0
            private set
        var flushed = false
            private set

        override fun sendOrder(key: String, event: OrderLifecycleEvent) {
            orderSent++
        }

        override fun sendBatch(key: String, event: BatchLifecycleEvent) {
            batchSent++
        }

        override fun sendTelemetry(key: String, event: CauldronTelemetryEvent) {
            telemetrySent++
        }

        override fun flush() {
            flushed = true
        }

        override fun logSnapshot(reason: String) = Unit
    }
}
