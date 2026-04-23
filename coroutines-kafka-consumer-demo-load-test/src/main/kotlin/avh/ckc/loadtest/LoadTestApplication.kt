    package avh.ckc.loadtest

import avh.ckc.loadtest.config.LoadTestConfig
import avh.ckc.loadtest.domain.OrderLifecycleStateMachine
import avh.ckc.loadtest.generator.TrafficGenerator
import avh.ckc.loadtest.kafka.LoadTestProducers
import avh.ckc.loadtest.runtime.ShardContext
import avh.ckc.loadtest.scenario.LoadScenario
import avh.ckc.loadtest.scenario.ScenarioEvaluationContext
import kotlinx.coroutines.runBlocking
import java.time.Instant

fun main() = runBlocking {
    val shardContext = ShardContext.fromEnvironment()
    val config = LoadTestConfig.fromEnvironment()
    val scenario = LoadScenario.parse(config.loadProfile)

    val now = Instant.now()
    val effectiveStart = shardContext.testRunStartedAt ?: now
    val currentPhase = scenario.phaseAt(now, effectiveStart, ScenarioEvaluationContext(config.baseRate))
    val preview = OrderLifecycleStateMachine(shardContext).createOrderBatch(orderIndex = 1, batchSlot = 0)

    println("load-test shard=${shardContext.shardIndex}/${shardContext.totalShards} runId=${shardContext.testRunId ?: "local"}")
    println("phase=${currentPhase?.name ?: "completed"} rate=${currentPhase?.currentRate() ?: 0.0} baseRate=${config.baseRate}")
    println("preview lifecycle events=${preview.lifecycleEvents.map { it.eventType.name }}")

    LoadTestProducers(config).use { producers ->
        TrafficGenerator(
            shardContext = shardContext,
            config = config,
            scenario = scenario,
            producers = producers
        ).run()
    }
}
