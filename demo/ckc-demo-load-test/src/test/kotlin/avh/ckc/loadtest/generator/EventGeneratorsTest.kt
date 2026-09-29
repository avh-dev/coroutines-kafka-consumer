package avh.ckc.loadtest.generator

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.BatchLifecycleEventType
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.loadtest.config.LoadTestConfig
import avh.ckc.loadtest.config.TelemetrySourceMode
import avh.ckc.loadtest.domain.LoadTestEventFactory
import avh.ckc.loadtest.domain.SimulationState
import avh.ckc.loadtest.kafka.LoadTestPublisher
import avh.ckc.loadtest.runtime.GeneratorIdentity
import java.time.Duration
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class EventGeneratorsTest {
    @Test
    fun `total emitted includes prerequisite publications`() {
        assertEquals(6, EmitResult(emitted = true, emittedCount = 2, delegated = 4).totalEmitted)
        assertEquals(3, EmitResult(emitted = false, delegated = 3, blocked = true).totalEmitted)
    }

    @Test
    fun `brewing step generator emits configured same-key burst`() {
        val publisher = RecordingPublisher()
        val fixture = brewingStepGenerator(
            publisher = publisher,
            config = config(
                minBrewingSteps = 5,
                maxBrewingSteps = 5,
                brewingStepBurstEvery = 1,
                minBrewingStepBurst = 3,
                maxBrewingStepBurst = 3
            )
        )

        val result = fixture.generator.emit(Instant.parse("2026-06-05T10:00:00Z"))

        val steps = publisher.batchEvents.filter { it.event.eventType == BatchLifecycleEventType.BATCH_BREWING_STEP_COMPLETED }
        assertEquals(3, result.emittedCount)
        assertEquals(3, steps.size)
        assertEquals(1, steps.map { it.key }.toSet().size)
        assertEquals(listOf(1, 2, 3), steps.map { it.event.getBatchBrewingStepCompleted().stepNumber })
        assertTrue(steps.map { it.event.metadata.eventId }.toSet().size == steps.size)
        assertEquals(1, fixture.state.snapshot().brewingBatches)
    }

    @Test
    fun `brewing step burst is capped by remaining batch steps`() {
        val publisher = RecordingPublisher()
        val fixture = brewingStepGenerator(
            publisher = publisher,
            config = config(
                minBrewingSteps = 2,
                maxBrewingSteps = 2,
                brewingStepBurstEvery = 1,
                minBrewingStepBurst = 5,
                maxBrewingStepBurst = 5
            )
        )

        val result = fixture.generator.emit(Instant.parse("2026-06-05T10:00:00Z"))

        val steps = publisher.batchEvents.filter { it.event.eventType == BatchLifecycleEventType.BATCH_BREWING_STEP_COMPLETED }
        assertEquals(2, result.emittedCount)
        assertEquals(2, steps.size)
        assertEquals(listOf(1, 2), steps.map { it.event.getBatchBrewingStepCompleted().stepNumber })
        assertEquals(0, fixture.state.snapshot().brewingBatches)
        assertEquals(1, fixture.state.snapshot().brewingCompletedBatches)
    }

    @Test
    fun `repeated brewing bursts keep queued batches bounded by active cauldrons`() {
        val fixture = brewingStepGenerator(
            publisher = RecordingPublisher(),
            config = config(
                minBrewingSteps = 20,
                maxBrewingSteps = 20,
                brewingStepBurstEvery = 1,
                minBrewingStepBurst = 5,
                maxBrewingStepBurst = 5
            )
        )

        repeat(40) { iteration ->
            fixture.generator.emit(Instant.parse("2026-06-05T10:00:00Z").plusMillis(iteration.toLong()))
            val snapshot = fixture.state.snapshot()
            assertTrue(
                snapshot.brewingBatches + snapshot.brewingCompletedBatches <= snapshot.activeCauldrons,
                "Brewing queues escaped the active-cauldron bound after iteration $iteration: $snapshot"
            )
        }
    }

    private fun brewingStepGenerator(
        publisher: LoadTestPublisher,
        config: LoadTestConfig
    ): BrewingStepFixture {
        val identity = GeneratorIdentity(externalShardIndex = 0, totalExternalShards = 1, workerIndex = 0, totalWorkers = 1)
        val state = SimulationState(cauldronCount = config.cauldronCount, identity = identity)
        val generator = eventGenerators(
            config = config,
            state = state,
            factory = LoadTestEventFactory(identity),
            publisher = publisher
        ).single { it.name == "batch_brewing_step_completed" }
        return BrewingStepFixture(generator, state)
    }

    private fun config(
        minBrewingSteps: Int,
        maxBrewingSteps: Int,
        brewingStepBurstEvery: Int,
        minBrewingStepBurst: Int,
        maxBrewingStepBurst: Int
    ): LoadTestConfig =
        LoadTestConfig(
            bootstrapServers = "localhost:9092",
            orderEventsTopic = "order.events.v1",
            batchEventsTopic = "batch.events.v1",
            cauldronEventsTopic = "cauldron.events.v1",
            baseTps = 100,
            orderEventPercent = 0,
            batchEventPercent = 100,
            cauldronTelemetryPercent = 0,
            loadProfile = "100 -> (1s, steady) -> 100",
            cauldronCount = 1,
            minOrdersPerBatch = 1,
            maxOrdersPerBatch = 1,
            minBrewingSteps = minBrewingSteps,
            maxBrewingSteps = maxBrewingSteps,
            brewingStepBurstEvery = brewingStepBurstEvery,
            minBrewingStepBurst = minBrewingStepBurst,
            maxBrewingStepBurst = maxBrewingStepBurst,
            maxBurst = 100,
            statsLogInterval = Duration.ofSeconds(30),
            diagnosticsBlobSize = 0,
            telemetrySourceMode = TelemetrySourceMode.ACTIVE_BATCHES,
            publishEnabled = true,
            auditLogEnabled = false
        )

    private class RecordingPublisher : LoadTestPublisher {
        val batchEvents = mutableListOf<BatchRecord>()

        override fun sendOrder(key: String, event: OrderLifecycleEvent) = Unit

        override fun sendBatch(key: String, event: BatchLifecycleEvent) {
            batchEvents += BatchRecord(key, event)
        }

        override fun sendTelemetry(key: String, event: CauldronTelemetryEvent) = Unit

        override fun flush() = Unit

        override fun logSnapshot(reason: String) = Unit
    }

    private data class BatchRecord(
        val key: String,
        val event: BatchLifecycleEvent
    )

    private data class BrewingStepFixture(
        val generator: EventGenerator,
        val state: SimulationState
    )
}
