package avh.ckc.demo.config

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean
import org.springframework.core.io.FileSystemResource
import kotlin.test.assertTrue

class ThreadStatsRulesConfigurationTest {
    @Test
    fun `application config classifies Spring Kafka and support threads`() {
        val rules = loadConfiguredRules()

        assertTrue(rules.contains(ThreadStatsRule("spring-kafka-consumer", "spring-kafka-consumer-order-lifecycle-")))
        assertTrue(rules.contains(ThreadStatsRule("spring-kafka-consumer", "spring-kafka-consumer-batch-lifecycle-")))
        assertTrue(rules.contains(ThreadStatsRule("spring-kafka-consumer", "spring-kafka-consumer-cauldron-telemetry-")))
        assertTrue(rules.contains(ThreadStatsRule("spring-kafka-thread-pool-worker", "spring-kafka-thread-pool-")))
        assertTrue(rules.contains(ThreadStatsRule("spring-kafka-virtual-worker", "spring-kafka-virtual-thread-pool-")))
        assertTrue(rules.contains(ThreadStatsRule("spring-task-scheduler", "ThreadPoolTaskScheduler-")))
        assertTrue(rules.contains(ThreadStatsRule("virtual-thread-runtime", "ForkJoinPool-")))
        assertTrue(rules.contains(ThreadStatsRule("virtual-thread-runtime", "VirtualThread-unparker")))
        assertTrue(rules.contains(ThreadStatsRule("jvm-fork-join", "ForkJoinPool.commonPool-")))
        assertTrue(rules.contains(ThreadStatsRule("audit", "logback-appender-AUDIT_TCP-")))
        assertTrue(rules.contains(ThreadStatsRule("jdk-http-client", "HttpClient-")))
        assertTrue(rules.contains(ThreadStatsRule("spring-lifecycle", "startstop-support-")))
        assertTrue(rules.contains(ThreadStatsRule("jvm-system", "DestroyJavaVM")))
    }

    private fun loadConfiguredRules(): Set<ThreadStatsRule> {
        val properties = YamlPropertiesFactoryBean().apply {
            setResources(FileSystemResource("src/main/resources/application.yml"))
        }.`object` ?: error("Failed to load demo application.yml")

        val ruleIndexes = properties.stringPropertyNames()
            .mapNotNull { name -> Regex("""thread-stats\.rules\[(\d+)]\.group""").matchEntire(name)?.groupValues?.get(1) }
            .map(String::toInt)

        return ruleIndexes.map { index ->
            ThreadStatsRule(
                group = properties.getProperty("thread-stats.rules[$index].group"),
                value = properties.getProperty("thread-stats.rules[$index].value")
            )
        }.toSet()
    }

    private data class ThreadStatsRule(
        val group: String,
        val value: String
    )
}
