package avh.ckc.demo.config

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean
import org.springframework.core.io.FileSystemResource
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ThreadStatsRulesConfigurationTest {
    @Test
    fun `application config classifies Spring Kafka and support threads`() {
        val rules = loadConfiguredRules()

        assertTrue(rules.contains(ThreadStatsRule("kafka", "spring-kafka-consumer", "spring-kafka-consumer-order-lifecycle-")))
        assertTrue(rules.contains(ThreadStatsRule("kafka", "spring-kafka-consumer", "spring-kafka-consumer-batch-lifecycle-")))
        assertTrue(rules.contains(ThreadStatsRule("kafka", "spring-kafka-consumer", "spring-kafka-consumer-cauldron-telemetry-")))
        assertTrue(rules.contains(ThreadStatsRule("business", "spring-kafka-thread-pool-worker", "spring-kafka-thread-pool-")))
        assertTrue(rules.contains(ThreadStatsRule("business", "spring-kafka-virtual-worker", "spring-kafka-virtual-thread-pool-")))
        assertTrue(rules.contains(ThreadStatsRule("other", "spring-task-scheduler", "ThreadPoolTaskScheduler-")))
        assertTrue(rules.contains(ThreadStatsRule("business", "confluent-parallel", "confluent-parallel-")))
        assertTrue(rules.contains(ThreadStatsRule("business", "confluent-parallel-worker", "pc-reactor-worker-")))
        assertTrue(rules.contains(ThreadStatsRule("kafka", "kafka-client", "pc-broker-poll")))
        assertTrue(rules.contains(ThreadStatsRule("kafka", "kafka-client", "pc-control")))
        assertTrue(rules.contains(ThreadStatsRule("kafka", "kafka-client", "pc-pool-")))
        assertTrue(rules.contains(ThreadStatsRule("kafka", "kafka-client", "micrometer-kafka-metrics")))
        assertTrue(rules.contains(ThreadStatsRule("business", "virtual-thread-runtime", "ForkJoinPool-")))
        assertTrue(rules.contains(ThreadStatsRule("business", "virtual-thread-runtime", "VirtualThread-unparker")))
        assertTrue(rules.contains(ThreadStatsRule("other", "jvm-fork-join", "ForkJoinPool.commonPool-")))
        assertTrue(rules.contains(ThreadStatsRule("audit", "audit", "logback-appender-AUDIT_TCP-")))
        assertTrue(rules.contains(ThreadStatsRule("http-client", "jdk-http-client", "HttpClient-")))
        assertTrue(rules.contains(ThreadStatsRule("other", "spring-lifecycle", "startstop-support-")))
        assertTrue(rules.contains(ThreadStatsRule("other", "jvm-system", "DestroyJavaVM")))
        assertEquals(
            listOf("business", "kafka", "redis-client", "http-client", "audit", "other"),
            loadConfiguredCategoryNames(),
        )
        assertEquals(
            "true",
            loadProperties().getProperty("thread-stats.metrics.category-order-prefix-enabled"),
        )
    }

    private fun loadConfiguredCategoryNames(): List<String> {
        val properties = loadProperties()
        return properties.stringPropertyNames()
            .mapNotNull { name -> Regex("""thread-stats\.categories\[(\d+)]\.name""").matchEntire(name)?.groupValues?.get(1) }
            .map(String::toInt)
            .sorted()
            .map { categoryIndex -> properties.getProperty("thread-stats.categories[$categoryIndex].name") }
    }

    private fun loadConfiguredRules(): Set<ThreadStatsRule> {
        val properties = loadProperties()

        val ruleCoordinates = properties.stringPropertyNames()
            .mapNotNull { name ->
                Regex("""thread-stats\.categories\[(\d+)]\.groups\[(\d+)]\.name""")
                    .matchEntire(name)
                    ?.destructured
                    ?.let { (category, group) -> category.toInt() to group.toInt() }
            }

        return ruleCoordinates.flatMap { (categoryIndex, groupIndex) ->
            val prefix = "thread-stats.categories[$categoryIndex].groups[$groupIndex]"
            val ruleIndexes = properties.stringPropertyNames()
                .mapNotNull { name -> Regex("""${Regex.escape(prefix)}\.rules\[(\d+)]\.value""").matchEntire(name)?.groupValues?.get(1) }
                .map(String::toInt)
            ruleIndexes.map { ruleIndex ->
                ThreadStatsRule(
                    category = properties.getProperty("thread-stats.categories[$categoryIndex].name"),
                    group = properties.getProperty("$prefix.name"),
                    value = properties.getProperty("$prefix.rules[$ruleIndex].value")
                )
            }
        }.toSet()
    }

    private fun loadProperties() =
        YamlPropertiesFactoryBean().apply {
            setResources(FileSystemResource("src/main/resources/application.yml"))
        }.`object` ?: error("Failed to load demo application.yml")

    private data class ThreadStatsRule(
        val category: String,
        val group: String,
        val value: String
    )
}
