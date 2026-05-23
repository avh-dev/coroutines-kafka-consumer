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
    val lifecyclePhase = scenario.phaseAt(now, effectiveStart, ScenarioEvaluationContext(config.lifecycleBaseRate))
    val telemetryPhase = scenario.phaseAt(now, effectiveStart, ScenarioEvaluationContext(config.telemetryBaseRate))
    val preview = OrderLifecycleStateMachine(shardContext).createOrderBatch(orderIndex = 1, batchSlot = 0)

    println("load-test shard=${shardContext.shardIndex}/${shardContext.totalShards} runId=${shardContext.testRunId ?: "local"}")
    println("bootstrapServers=${config.bootstrapServers}")
    println("topics order=${config.orderEventsTopic} batch=${config.batchEventsTopic} cauldron=${config.cauldronEventsTopic}")
    println(
        "phase=${lifecyclePhase?.name ?: "completed"} " +
            "lifecycleRate=${lifecyclePhase?.currentRate() ?: 0.0} lifecycleBaseRate=${config.lifecycleBaseRate} " +
            "telemetryRate=${telemetryPhase?.currentRate() ?: 0.0} telemetryBaseRate=${config.telemetryBaseRate}"
    )
    println("preview order events=${preview.orderEvents.map { it.eventType.name }}")
    println("preview batch events=${preview.batchEvents.map { it.eventType.name }}")

    LoadTestProducers(config).use { producers ->
        TrafficGenerator(
            shardContext = shardContext,
            config = config,
            scenario = scenario,
            producers = producers
        ).run()
    }
}
