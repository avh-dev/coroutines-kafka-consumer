package avh.ckc.spring

import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.actuate.health.Status
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean

@AutoConfiguration(after = [CkcSpringBootAutoConfiguration::class])
@ConditionalOnClass(HealthIndicator::class)
@ConditionalOnBean(CkcConsumersLifecycle::class)
@ConditionalOnProperty(prefix = "ckc.health", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class CkcSpringBootHealthAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean(name = ["ckcHealthIndicator"])
    fun ckcHealthIndicator(lifecycle: CkcConsumersLifecycle): HealthIndicator =
        CkcHealthIndicator(lifecycle)
}

private class CkcHealthIndicator(
    private val lifecycle: CkcConsumersLifecycle
) : HealthIndicator {
    override fun health(): Health {
        val consumers = lifecycle.consumerStateSnapshots()
        val unavailableAutoStartupConsumers = consumers
            .filter { it.autoStartup && !it.running }
            .map { it.name }
        val status = if (lifecycle.lifecycleStarted && unavailableAutoStartupConsumers.isNotEmpty()) {
            Status.OUT_OF_SERVICE
        } else {
            Status.UP
        }

        return Health.status(status)
            .withDetail("lifecycleStarted", lifecycle.lifecycleStarted)
            .withDetail("runningConsumers", consumers.count { it.running })
            .withDetail("registeredConsumers", consumers.size)
            .withDetail("unavailableAutoStartupConsumers", unavailableAutoStartupConsumers)
            .withDetail("consumers", consumers.associate { snapshot -> snapshot.name to snapshot.details() })
            .build()
    }

    private fun CkcConsumerStateSnapshot.details(): Map<String, Any?> =
        linkedMapOf(
            "autoStartup" to autoStartup,
            "running" to running,
            "handler" to handler,
            "cluster" to cluster,
            "topics" to topics,
            "topicPattern" to topicPattern,
            "groupId" to groupId,
            "clientId" to clientId,
            "processingMode" to processingMode,
            "workerConcurrency" to workerConcurrency,
            "consumerPollLoopConcurrency" to consumerPollLoopConcurrency,
            "processingDispatcher" to processingDispatcher,
            "retrySchema" to retrySchema,
            "metrics" to metrics
        )
}
