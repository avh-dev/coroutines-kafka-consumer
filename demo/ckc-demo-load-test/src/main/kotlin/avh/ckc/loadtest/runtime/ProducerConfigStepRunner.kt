package avh.ckc.loadtest.runtime

import avh.ckc.loadtest.config.ProducerConfigStep
import avh.ckc.loadtest.kafka.ProducerConfigTarget
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.time.Duration
import java.time.Instant

class ProducerConfigStepRunner(
    private val steps: List<ProducerConfigStep>,
    private val runStartedAt: Instant,
    private val producers: ProducerConfigTarget,
    private val events: LoadTestExperimentEvents
) {
    suspend fun run() {
        steps.forEachIndexed { index, step ->
            waitUntil(runStartedAt.plusSeconds(step.atSeconds))
            val eventId = "producer-config-${index + 1}"
            events.record(eventId, "producer_config", "started", step)
            try {
                withContext(Dispatchers.IO) { producers.reconfigure(step) }
                events.record(eventId, "producer_config", "succeeded", step)
            } catch (error: Throwable) {
                events.record(eventId, "producer_config", "failed", step, error.message)
                throw error
            }
        }
    }

    private suspend fun waitUntil(target: Instant) {
        val remaining = Duration.between(Instant.now(), target).toMillis()
        if (remaining > 0) delay(remaining)
    }
}

class LoadTestExperimentEvents(
    private val shardContext: ShardContext,
    private val clock: () -> Instant = Instant::now,
    private val output: (String) -> Unit = ::println
) {
    fun record(
        eventId: String,
        type: String,
        status: String,
        step: ProducerConfigStep,
        error: String? = null
    ) {
        val event = LoadTestExperimentEvent(
            eventId = eventId,
            runId = shardContext.testRunId ?: "local",
            shard = shardContext.shardIndex,
            timestamp = clock().toString(),
            type = type,
            status = status,
            producerConfig = step,
            error = error
        )
        output("$EVENT_PREFIX${Json.encodeToString(event)}")
    }

    companion object {
        const val EVENT_PREFIX = "CKC_EXPERIMENT_EVENT "
    }
}

@Serializable
data class LoadTestExperimentEvent(
    val eventId: String,
    val runId: String,
    val shard: Int,
    val timestamp: String,
    val type: String,
    val status: String,
    val producerConfig: ProducerConfigStep,
    val error: String? = null
)
