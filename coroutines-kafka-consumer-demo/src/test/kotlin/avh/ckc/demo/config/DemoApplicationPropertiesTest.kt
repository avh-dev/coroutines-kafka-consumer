package avh.ckc.demo.config

import kotlin.test.Test
import kotlin.test.assertEquals
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource

class DemoApplicationPropertiesTest {
    @Test
    fun `consumer runtime defaults preserve existing demo settings`() {
        val properties = bind(emptyMap())

        assertEquals(2, properties.consumers.lifecycle.workerConcurrency)
        assertEquals(1, properties.consumers.lifecycle.pollLoopConcurrency)
        assertEquals(1024, properties.consumers.lifecycle.workChannelCapacity)
        assertEquals(4, properties.consumers.telemetry.workerConcurrency)
        assertEquals(1, properties.consumers.telemetry.pollLoopConcurrency)
        assertEquals(256, properties.consumers.telemetry.workChannelCapacity)
    }

    @Test
    fun `consumer runtime settings can be overridden`() {
        val properties = bind(
            mapOf(
                "demo.consumers.lifecycle.worker-concurrency" to "12",
                "demo.consumers.lifecycle.poll-loop-concurrency" to "3",
                "demo.consumers.lifecycle.work-channel-capacity" to "2048",
                "demo.consumers.telemetry.worker-concurrency" to "8",
                "demo.consumers.telemetry.poll-loop-concurrency" to "2",
                "demo.consumers.telemetry.work-channel-capacity" to "512"
            )
        )

        assertEquals(12, properties.consumers.lifecycle.workerConcurrency)
        assertEquals(3, properties.consumers.lifecycle.pollLoopConcurrency)
        assertEquals(2048, properties.consumers.lifecycle.workChannelCapacity)
        assertEquals(8, properties.consumers.telemetry.workerConcurrency)
        assertEquals(2, properties.consumers.telemetry.pollLoopConcurrency)
        assertEquals(512, properties.consumers.telemetry.workChannelCapacity)
    }

    private fun bind(values: Map<String, String>): DemoApplicationProperties =
        Binder(MapConfigurationPropertySource(values))
            .bind("demo", DemoApplicationProperties::class.java)
            .orElseGet(::DemoApplicationProperties)
}
