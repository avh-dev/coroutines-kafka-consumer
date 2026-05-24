package avh.ckc.loadtest

import avh.ckc.loadtest.config.LoadTestConfig
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
    val phase = scenario.phaseAt(now, effectiveStart, ScenarioEvaluationContext(config.baseTps))

    println("load-test shard=${shardContext.shardIndex}/${shardContext.totalShards} runId=${shardContext.testRunId ?: "local"}")
    println("bootstrapServers=${config.bootstrapServers}")
    println("topics order=${config.orderEventsTopic} batch=${config.batchEventsTopic} cauldron=${config.cauldronEventsTopic}")
    println(
        "phase=${phase?.name ?: "completed"} baseTps=${config.baseTps} currentTps=${phase?.currentRate() ?: 0.0} " +
            "mix(order=${config.orderEventPercent},batch=${config.batchEventPercent},cauldron=${config.cauldronTelemetryPercent})"
    )

    LoadTestProducers(config).use { producers ->
        TrafficGenerator(
            shardContext = shardContext,
            config = config,
            scenario = scenario,
            producers = producers
        ).run()
    }
}
