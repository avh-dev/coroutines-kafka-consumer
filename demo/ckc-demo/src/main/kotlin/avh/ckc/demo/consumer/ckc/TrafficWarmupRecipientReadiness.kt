package avh.ckc.demo.consumer.ckc

import avh.ckc.demo.config.DemoApplicationProperties
import com.linecorp.armeria.common.HttpResponse
import com.linecorp.armeria.common.HttpStatus
import com.linecorp.armeria.server.annotation.Get
import com.linecorp.armeria.server.annotation.PathPrefix
import org.springframework.stereotype.Component
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import java.util.concurrent.atomic.AtomicReference

@Component
class TrafficWarmupRecipientReadiness(
    properties: DemoApplicationProperties
) : HealthIndicator {
    private val managedStartup = properties.trafficWarmup.role in setOf(
        DemoApplicationProperties.TrafficWarmupRole.RECIPIENT,
        DemoApplicationProperties.TrafficWarmupRole.AUTOMATIC
    )
    private val state = AtomicReference(if (managedStartup) State.STARTING else State.NOT_APPLICABLE)

    fun isReady(): Boolean = state.get() in setOf(State.READY, State.NOT_APPLICABLE)

    fun markReady() {
        if (managedStartup) state.set(State.READY)
    }

    fun markFailed() {
        if (managedStartup) state.set(State.FAILED)
    }

    fun markStopped() {
        if (managedStartup) state.set(State.STOPPED)
    }

    override fun health(): Health = if (isReady()) {
        Health.up().build()
    } else {
        Health.down().withDetail("state", state.get().name).build()
    }

    private enum class State {
        NOT_APPLICABLE,
        STARTING,
        READY,
        FAILED,
        STOPPED
    }
}

@Component
@PathPrefix("/internal/traffic-warmup")
class TrafficWarmupRecipientReadinessHttpService(
    private val readiness: TrafficWarmupRecipientReadiness
) {
    @Get("/ready")
    fun ready(): HttpResponse = HttpResponse.of(
        if (readiness.isReady()) HttpStatus.OK else HttpStatus.SERVICE_UNAVAILABLE
    )
}
