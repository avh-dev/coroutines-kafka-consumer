package avh.ckc.loadtest.generator

import avh.ckc.loadtest.config.LoadTestConfig
import avh.ckc.loadtest.domain.LoadTestEventFactory
import avh.ckc.loadtest.domain.SimulationState
import avh.ckc.loadtest.kafka.LoadTestPublisher
import avh.ckc.loadtest.runtime.GeneratorIdentity
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
    totalWorkers: Int = 1
) {
    private val identity = GeneratorIdentity.from(shardContext, workerIndex, totalWorkers)
    private val state = SimulationState(config.cauldronCount, config.fakeEntityPrefix, identity)
    private val stats = TrafficStats()

    suspend fun run(flushOnCompletion: Boolean = true) = coroutineScope {
        val startedAt = shardContext.testRunStartedAt ?: Instant.now()
        val factory = LoadTestEventFactory(identity)
        val generators = eventGenerators(config, state, factory, producers)
        val topicWeights = generators
            .groupBy(EventGenerator::topic)
            .mapValues { (_, topicGenerators) -> topicGenerators.sumOf(EventGenerator::weight) }

        val jobs = generators.map { generator ->
            launch {
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
        val logger = launch {
            while (true) {
                delay(config.statsLogInterval.toMillis())
                producers.logSnapshot("${identity.label()} ${stats.format(state.snapshot())}")
            }
        }

        jobs.forEach { it.join() }
        logger.cancel()
        producers.logSnapshot("${identity.label()} ${stats.format(state.snapshot())}")
        if (flushOnCompletion) {
            producers.flush()
        }
    }
}
