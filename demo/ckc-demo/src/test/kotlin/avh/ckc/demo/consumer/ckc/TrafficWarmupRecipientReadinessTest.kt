package avh.ckc.demo.consumer.ckc

import avh.ckc.demo.config.DemoApplicationProperties
import com.linecorp.armeria.common.HttpStatus
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TrafficWarmupRecipientReadinessTest {
    @Test
    fun `recipient stays down until every consumer readiness wait has completed`() {
        val properties = DemoApplicationProperties().also {
            it.trafficWarmup.role = DemoApplicationProperties.TrafficWarmupRole.RECIPIENT
        }
        val readiness = TrafficWarmupRecipientReadiness(properties)

        val endpoint = TrafficWarmupRecipientReadinessHttpService(readiness)
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, endpoint.ready().aggregate().join().status())

        readiness.markReady()

        assertEquals(HttpStatus.OK, endpoint.ready().aggregate().join().status())
    }

    @Test
    fun `non-recipient role does not block application health`() {
        val readiness = TrafficWarmupRecipientReadiness(DemoApplicationProperties())

        assertEquals(true, readiness.isReady())
    }

    @Test
    fun `automatic warm-up is unhealthy until consumers become ready and after failure`() {
        val properties = DemoApplicationProperties().also {
            it.trafficWarmup.role = DemoApplicationProperties.TrafficWarmupRole.AUTOMATIC
        }
        val readiness = TrafficWarmupRecipientReadiness(properties)

        assertEquals(false, readiness.isReady())
        assertEquals("DOWN", readiness.health().status.code)

        readiness.markReady()
        assertEquals("UP", readiness.health().status.code)

        readiness.markFailed()
        assertEquals(false, readiness.isReady())
        assertEquals("DOWN", readiness.health().status.code)
    }
}
