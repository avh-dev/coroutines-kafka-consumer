package avh.ckc.loadtest.runtime

import avh.ckc.loadtest.config.ProducerConfigStep
import avh.ckc.loadtest.config.ProducerTopic
import avh.ckc.loadtest.kafka.ProducerConfigTarget
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals

class ProducerConfigStepRunnerTest {
    @Test
    fun `applies step and emits actual start and success events`() = runBlocking {
        val applied = mutableListOf<ProducerConfigStep>()
        val output = mutableListOf<String>()
        val now = Instant.parse("2026-08-27T10:15:30Z")
        val shard = ShardContext(0, 1, "run-7", now)
        val step = ProducerConfigStep(0, ProducerTopic.TELEMETRY, lingerMs = 50)
        val events = LoadTestExperimentEvents(shard, clock = { now }, output = output::add)

        ProducerConfigStepRunner(
            listOf(step),
            now,
            ProducerConfigTarget(applied::add),
            events
        ).run()

        assertEquals(listOf(step), applied)
        assertEquals(2, output.size)
        assertEquals(listOf("started", "succeeded"), output.map(::eventStatus))
        assertEquals("producer-config-1", eventJson(output.first())["eventId"]?.jsonPrimitive?.content)
        assertEquals("2026-08-27T10:15:30Z", eventJson(output.first())["timestamp"]?.jsonPrimitive?.content)
    }

    private fun eventStatus(line: String): String =
        eventJson(line)["status"]!!.jsonPrimitive.content

    private fun eventJson(line: String) = Json.parseToJsonElement(
        line.removePrefix(LoadTestExperimentEvents.EVENT_PREFIX)
    ).jsonObject
}
