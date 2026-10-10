package avh.ckc.loadtest.generator

import avh.ckc.loadtest.config.LoadTestConfig
import avh.ckc.loadtest.domain.LoadTestEventFactory
import avh.ckc.loadtest.domain.SimulationState
import avh.ckc.loadtest.kafka.LoadTestPublisher
import avh.ckc.loadtest.runtime.GeneratorIdentity
import avh.ckc.loadtest.runtime.ScheduledStartGate
import avh.ckc.loadtest.runtime.ShardContext
import avh.ckc.loadtest.scenario.LoadScenario
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.time.Instant

class TrafficGenerator(
    private val shardContext: ShardContext,
    private val config: LoadTestConfig,
    private val scenario: LoadScenario,
    private val producers: LoadTestPublisher,
    workerIndex: Int = 0,
    totalWorkers: Int = 1,
    private val scheduledStartGate: ScheduledStartGate = ScheduledStartGate()
) {
    private val identity = GeneratorIdentity.from(shardContext, workerIndex, totalWorkers)
    private val state = SimulationState(config.cauldronCount, identity)
    private val stats = TrafficStats()

    suspend fun run(flushOnCompletion: Boolean = true) = coroutineScope {
        val factory = LoadTestEventFactory(identity)
        val generators = eventGenerators(config, state, factory, producers)
        val topicWeights = generators
            .groupBy(EventGenerator::topic)
            .mapValues { (_, topicGenerators) -> topicGenerators.sumOf(EventGenerator::weight) }

        producers.prepare()
        val startedAt = awaitScheduledStart()
        val jobs = generators.map { generator ->
            launch {
                if (generator is FleetTelemetryEventGenerator) {
                    FleetTelemetryGeneratorRunner(generator, config, scenario, startedAt, stats).run()
                } else {
                    RateControlledGeneratorRunner(
                        generator = generator,
                        config = config,
                        scenario = scenario,
                        startedAt = startedAt,
                        topicWeightTotal = topicWeights.getValue(generator.topic),
                        stats = stats
                    ).run()
                }
            }
        }
        val logger = launch {
            while (true) {
                delay(config.statsLogInterval.toMillis())
                producers.logSnapshot("${identity.label()} ${stats.format(state.snapshot())}")
            }
        }

        jobs.forEach { it.join() }
        logger.cancel()
        println("load-test lifecycle generation_completed_at=${Instant.now()} ${identity.label()}")
        producers.logSnapshot("${identity.label()} ${stats.format(state.snapshot())}")
        if (flushOnCompletion) {
            producers.flush()
            println("load-test lifecycle producer_flush_completed_at=${Instant.now()} ${identity.label()}")
        }
    }

    private suspend fun awaitScheduledStart(): Instant {
        val scheduledAt = shardContext.testRunStartedAt ?: Instant.now()
        println(
            "load-test lifecycle armed run_id=${shardContext.testRunId ?: "local"} " +
                "attempt_id=${shardContext.launchAttemptId} ${identity.label()} scheduled_start=$scheduledAt"
        )
        val activatedAt = scheduledStartGate.await(scheduledAt)
        println("load-test lifecycle profile_activated_at=$activatedAt scheduled_start=$scheduledAt ${identity.label()}")
        return scheduledAt
    }

}
