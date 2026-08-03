package avh.ckc.demo.config

import avh.ckc.demo.config.DemoApplicationProperties.ProcessingDispatcherType
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class DemoProfileTimelineLabelTest {
    @Test
    fun `uses resolved default dispatcher names for coroutine profiles`() {
        val label = DemoProfileTimelineLabel(DemoApplicationProperties())

        assertEquals("ckc.fixed.8", label.resolve("ckc"))
        assertEquals("ckc-sync.io", label.resolve("ckc-sync"))
        assertEquals("spring-kafka-coroutines-naive.fixed.8", label.resolve("spring-kafka-coroutines-naive"))
        assertEquals("cpc-reactor.fixed.8", label.resolve("confluent-parallel-reactor"))
    }

    @Test
    fun `includes explicit dispatcher settings when they are meaningful`() {
        val properties = DemoApplicationProperties().apply {
            consumers.processingDispatcherType = ProcessingDispatcherType.FIXED
            consumers.workerDispatcherThreads = 1
        }
        val label = DemoProfileTimelineLabel(properties)

        assertEquals("ckc.fixed.1", label.resolve("ckc"))
    }

    @Test
    fun `uses compact default dispatcher suffix`() {
        val properties = DemoApplicationProperties().apply {
            consumers.processingDispatcherType = ProcessingDispatcherType.DEFAULT
        }
        val label = DemoProfileTimelineLabel(properties)

        assertEquals("ckc.default", label.resolve("ckc"))
    }

    @Test
    fun `keeps plain names for profiles without useful dispatcher variants`() {
        val label = DemoProfileTimelineLabel(DemoApplicationProperties())

        assertEquals("spring-kafka", label.resolve("spring-kafka"))
        assertEquals("spring-kafka-thread-pool", label.resolve("spring-kafka-thread-pool"))
        assertEquals("spring-kafka-virtual-thread-pool", label.resolve("spring-kafka-virtual-thread-pool"))
        assertEquals("cpc", label.resolve("confluent-parallel"))
        assertEquals("ckc-spring-boot", label.resolve("ckc-spring-boot"))
    }

    @Test
    fun `uses compact virtual thread suffix`() {
        val properties = DemoApplicationProperties().apply {
            consumers.processingDispatcherType = ProcessingDispatcherType.VIRTUAL
        }
        val label = DemoProfileTimelineLabel(properties)

        assertEquals("ckc-sync.virtual-thread", label.resolve("ckc-sync"))
    }
}
