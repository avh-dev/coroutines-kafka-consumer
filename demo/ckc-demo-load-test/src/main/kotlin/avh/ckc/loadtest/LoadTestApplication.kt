package avh.ckc.loadtest

import avh.ckc.loadtest.config.LoadTestConfig
import avh.ckc.loadtest.generator.TrafficGenerator
import avh.ckc.loadtest.kafka.LoadTestProducers
import avh.ckc.loadtest.kafka.ProducerPoolSizes
import avh.ckc.loadtest.metrics.LoadTestMetrics
import avh.ckc.loadtest.runtime.ShardContext
import avh.ckc.loadtest.runtime.effectiveGeneratorWorkers
import avh.ckc.loadtest.runtime.workerBaseTps
import avh.ckc.loadtest.runtime.LoadTestExperimentEvents
import avh.ckc.loadtest.runtime.ProducerConfigStepRunner
import avh.ckc.loadtest.scenario.LoadScenario
import avh.ckc.loadtest.scenario.ScenarioEvaluationContext
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.cancelAndJoin
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

fun main() = runBlocking {
    val shardContext = ShardContext.fromEnvironment()
    val config = LoadTestConfig.fromEnvironment()
    System.setProperty("AUDIT_LOG_ENABLED", config.auditLogEnabled.toString())
    System.setProperty("AUDIT_TCP_HOST", config.auditHost)
    System.setProperty("AUDIT_TCP_PORT", config.auditPort.toString())
    val scenario = LoadScenario.parse(config.loadProfile)
    val producerPoolSizes = ProducerPoolSizes.from(config, scenario)

    val now = Instant.now()
    val effectiveStart = shardContext.testRunStartedAt ?: now
    val phase = scenario.phaseAt(now, effectiveStart, ScenarioEvaluationContext(config.baseTps))
    val effectiveWorkers = effectiveGeneratorWorkers(config.baseTps, config.generatorWorkers)

    println("load-test externalShard=${shardContext.shardIndex}/${shardContext.totalShards} runId=${shardContext.testRunId ?: "local"}")
    println("bootstrapServers=${config.bootstrapServers}")
    println("topics order=${config.orderEventsTopic} batch=${config.batchEventsTopic} cauldron=${config.cauldronEventsTopic}")
    println(
        "phase=${phase?.name ?: "completed"} baseTps=${config.baseTps} currentTps=${phase?.currentRate() ?: 0.0} " +
            "mix(order=${config.orderEventPercent},batch=${config.batchEventPercent},cauldron=${config.cauldronTelemetryPercent})"
    )
    println("workers=$effectiveWorkers configuredWorkers=${config.generatorWorkers} baseTpsPerJvm=${config.baseTps}")
    println(
        "producerPools(order=${producerPoolSizes.order},batch=${producerPoolSizes.batch},telemetry=${producerPoolSizes.cauldronTelemetry}) " +
            "tpsPerProducer(order=${config.producerCapacity.orderTps},batch=${config.producerCapacity.batchTps}," +
            "telemetry=${config.producerCapacity.cauldronTelemetryTps})"
    )
    println(
        config.topicKafkaProducers.all().joinToString(prefix = "producerSettings ", separator = " ") { (topic, settings) ->
            "$topic(lingerMs=${settings.lingerMs},batchSize=${settings.batchSize}," +
                "compression=${settings.compressionType},bufferMemory=${settings.bufferMemory})"
        }
    )
    println("loadTestMetrics=http://0.0.0.0:${config.metricsPort}/metrics")

    LoadTestMetrics(config.metricsPort, shardContext.shardIndex, producerPoolSizes).use { metrics ->
        LoadTestProducers(config, shardContext, producerPoolSizes, metrics).use { producers ->
            runTrafficGenerators(shardContext, config, scenario, producers, effectiveWorkers, effectiveStart)
        }
    }
}

private suspend fun runTrafficGenerators(
    shardContext: ShardContext,
    config: LoadTestConfig,
    scenario: LoadScenario,
    producers: LoadTestProducers,
    workerCount: Int,
    runStartedAt: Instant
) {
    coroutineScope {
        val producerConfigJob = launch {
            ProducerConfigStepRunner(
                config.producerConfigSteps,
                runStartedAt,
                producers,
                LoadTestExperimentEvents(shardContext)
            ).run()
        }
        try {
            if (workerCount == 1) {
                TrafficGenerator(
                    shardContext = shardContext,
                    config = config.copy(baseTps = workerBaseTps(config.baseTps, workerIndex = 0, totalWorkers = 1)),
                    scenario = scenario,
                    producers = producers
                ).run()
            } else {
                newLoadGeneratorDispatcher(workerCount).use { dispatcher ->
                    (0 until workerCount).map { workerIndex ->
                        launch(dispatcher) {
                            TrafficGenerator(
                                shardContext = shardContext,
                                config = config.copy(
                                    baseTps = workerBaseTps(config.baseTps, workerIndex, workerCount)
                                ),
                                scenario = scenario,
                                producers = producers,
                                workerIndex = workerIndex,
                                totalWorkers = workerCount
                            ).run(flushOnCompletion = false)
                        }
                    }.joinAll()
                }
            }
        } finally {
            producerConfigJob.cancelAndJoin()
        }
    }
    producers.logSnapshot("workers-complete")
    producers.flush()
}

private fun newLoadGeneratorDispatcher(workerCount: Int) =
    Executors.newFixedThreadPool(workerCount) {
        val threadNumber = loadGeneratorThreadNumber.incrementAndGet()
        Thread(it, "load-generator-$threadNumber")
    }.asCoroutineDispatcher()

private val loadGeneratorThreadNumber = AtomicInteger()
