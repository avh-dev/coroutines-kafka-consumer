package avh.ckc.demo.config

import avh.ckc.demo.ml.ModelCallMetrics
import avh.ckc.demo.ml.eta.ArmeriaSuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.JdkSuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.JdkSyncArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SyncArcaneEtaModelClient
import avh.ckc.demo.ml.flavour.ArmeriaSuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.JdkSuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.JdkSyncOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SyncOrderFlavourModelClient
import avh.ckc.demo.registry.ArmeriaSuspendBrewingStepRegistryClient
import avh.ckc.demo.registry.JdkSuspendBrewingStepRegistryClient
import avh.ckc.demo.registry.JdkSyncBrewingStepRegistryClient
import avh.ckc.demo.registry.SuspendBrewingStepRegistryClient
import avh.ckc.demo.registry.SyncBrewingStepRegistryClient
import com.linecorp.armeria.client.WebClient
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.net.URI
import java.net.http.HttpClient
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

@Configuration(proxyBeanMethods = false)
class ModelClientConfiguration {
    @Bean
    fun modelCallMetrics(meterRegistry: MeterRegistry): ModelCallMetrics =
        ModelCallMetrics(meterRegistry)

    @Bean(destroyMethod = "shutdown")
    @Profile("spring-kafka", "spring-kafka-thread-pool", "spring-kafka-virtual-thread-pool", "confluent-parallel", "ckc-sync")
    @ConditionalOnProperty(prefix = "demo.model", name = ["jdk-http-client-executor"], havingValue = "virtual")
    fun syncJdkHttpClientVirtualExecutor(properties: DemoApplicationProperties): ExecutorService {
        val threadNumber = AtomicLong()
        val prefix = properties.model.jdkHttpClientVirtualThreadNamePrefix.ifBlank { "jdk-http-client-virtual-" }
        return Executors.newThreadPerTaskExecutor { runnable ->
            Thread.ofVirtual()
                .name(prefix, threadNumber.incrementAndGet())
                .factory()
                .newThread(runnable)
        }
    }

    @Bean
    @Profile("spring-kafka", "spring-kafka-thread-pool", "spring-kafka-virtual-thread-pool", "confluent-parallel", "ckc-sync")
    fun syncJdkHttpClient(
        @Qualifier("syncJdkHttpClientVirtualExecutor") virtualExecutorProvider: ObjectProvider<ExecutorService>
    ): HttpClient {
        val builder = HttpClient.newBuilder()
        virtualExecutorProvider.ifAvailable { executor -> builder.executor(executor) }
        return builder.build()
    }

    @Bean
    @Profile("spring-kafka", "spring-kafka-thread-pool", "spring-kafka-virtual-thread-pool", "confluent-parallel", "ckc-sync")
    fun syncArcaneEtaModelClient(
        properties: DemoApplicationProperties,
        syncJdkHttpClient: HttpClient,
        modelCallMetrics: ModelCallMetrics
    ): SyncArcaneEtaModelClient =
        JdkSyncArcaneEtaModelClient(
            URI.create(properties.etaModelBaseUrl()),
            httpClient = syncJdkHttpClient,
            modelCallMetrics = modelCallMetrics
        )

    @Bean
    @Profile("ckc", "ckc-spring-boot", "confluent-parallel-reactor", "spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.model", name = ["http-client"], havingValue = "armeria", matchIfMissing = true)
    fun armeriaEtaModelWebClient(properties: DemoApplicationProperties): WebClient =
        WebClient.of(properties.etaModelBaseUrl())

    @Bean
    @Profile("ckc", "ckc-spring-boot", "confluent-parallel-reactor", "spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.model", name = ["http-client"], havingValue = "armeria", matchIfMissing = true)
    fun armeriaFlavourModelWebClient(properties: DemoApplicationProperties): WebClient =
        WebClient.of(properties.flavourModelBaseUrl())

    @Bean
    @Profile("ckc", "ckc-spring-boot", "confluent-parallel-reactor", "spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.model", name = ["http-client"], havingValue = "armeria", matchIfMissing = true)
    fun armeriaRegistryWebClient(properties: DemoApplicationProperties): WebClient =
        WebClient.of(properties.registry.baseUrl)

    @Bean
    @Profile("ckc", "ckc-spring-boot", "confluent-parallel-reactor", "spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.model", name = ["http-client"], havingValue = "armeria", matchIfMissing = true)
    fun armeriaSuspendArcaneEtaModelClient(
        @Qualifier("armeriaEtaModelWebClient") armeriaEtaModelWebClient: WebClient,
        modelCallMetrics: ModelCallMetrics
    ): SuspendArcaneEtaModelClient =
        ArmeriaSuspendArcaneEtaModelClient(armeriaEtaModelWebClient, modelCallMetrics = modelCallMetrics)

    @Bean
    @Profile("ckc", "ckc-spring-boot", "confluent-parallel-reactor", "spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.model", name = ["http-client"], havingValue = "jdk")
    fun suspendJdkHttpClient(): HttpClient =
        HttpClient.newHttpClient()

    @Bean
    @Profile("ckc", "ckc-spring-boot", "confluent-parallel-reactor", "spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.model", name = ["http-client"], havingValue = "jdk")
    fun jdkSuspendArcaneEtaModelClient(
        properties: DemoApplicationProperties,
        @Qualifier("suspendJdkHttpClient") suspendJdkHttpClient: HttpClient,
        modelCallMetrics: ModelCallMetrics
    ): SuspendArcaneEtaModelClient =
        JdkSuspendArcaneEtaModelClient(
            URI.create(properties.etaModelBaseUrl()),
            httpClient = suspendJdkHttpClient,
            modelCallMetrics = modelCallMetrics
        )

    @Bean
    @Profile("spring-kafka", "spring-kafka-thread-pool", "spring-kafka-virtual-thread-pool", "confluent-parallel", "ckc-sync")
    fun syncOrderFlavourModelClient(
        properties: DemoApplicationProperties,
        syncJdkHttpClient: HttpClient,
        modelCallMetrics: ModelCallMetrics
    ): SyncOrderFlavourModelClient =
        JdkSyncOrderFlavourModelClient(
            URI.create(properties.flavourModelBaseUrl()),
            httpClient = syncJdkHttpClient,
            modelCallMetrics = modelCallMetrics
        )

    @Bean
    @Profile("ckc", "ckc-spring-boot", "confluent-parallel-reactor", "spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.model", name = ["http-client"], havingValue = "armeria", matchIfMissing = true)
    fun armeriaSuspendOrderFlavourModelClient(
        @Qualifier("armeriaFlavourModelWebClient") armeriaFlavourModelWebClient: WebClient,
        modelCallMetrics: ModelCallMetrics
    ): SuspendOrderFlavourModelClient =
        ArmeriaSuspendOrderFlavourModelClient(armeriaFlavourModelWebClient, modelCallMetrics = modelCallMetrics)

    @Bean
    @Profile("ckc", "ckc-spring-boot", "confluent-parallel-reactor", "spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.model", name = ["http-client"], havingValue = "jdk")
    fun jdkSuspendOrderFlavourModelClient(
        properties: DemoApplicationProperties,
        @Qualifier("suspendJdkHttpClient") suspendJdkHttpClient: HttpClient,
        modelCallMetrics: ModelCallMetrics
    ): SuspendOrderFlavourModelClient =
        JdkSuspendOrderFlavourModelClient(
            URI.create(properties.flavourModelBaseUrl()),
            httpClient = suspendJdkHttpClient,
            modelCallMetrics = modelCallMetrics
        )

    @Bean
    @Profile("spring-kafka", "spring-kafka-thread-pool", "spring-kafka-virtual-thread-pool", "confluent-parallel", "ckc-sync")
    fun syncBrewingStepRegistryClient(
        properties: DemoApplicationProperties,
        syncJdkHttpClient: HttpClient,
        modelCallMetrics: ModelCallMetrics
    ): SyncBrewingStepRegistryClient =
        JdkSyncBrewingStepRegistryClient(
            URI.create(properties.registry.baseUrl),
            httpClient = syncJdkHttpClient,
            modelCallMetrics = modelCallMetrics
        )

    @Bean
    @Profile("ckc", "ckc-spring-boot", "confluent-parallel-reactor", "spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.model", name = ["http-client"], havingValue = "armeria", matchIfMissing = true)
    fun armeriaSuspendBrewingStepRegistryClient(
        @Qualifier("armeriaRegistryWebClient") armeriaRegistryWebClient: WebClient,
        modelCallMetrics: ModelCallMetrics
    ): SuspendBrewingStepRegistryClient =
        ArmeriaSuspendBrewingStepRegistryClient(armeriaRegistryWebClient, modelCallMetrics = modelCallMetrics)

    @Bean
    @Profile("ckc", "ckc-spring-boot", "confluent-parallel-reactor", "spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.model", name = ["http-client"], havingValue = "jdk")
    fun jdkSuspendBrewingStepRegistryClient(
        properties: DemoApplicationProperties,
        @Qualifier("suspendJdkHttpClient") suspendJdkHttpClient: HttpClient,
        modelCallMetrics: ModelCallMetrics
    ): SuspendBrewingStepRegistryClient =
        JdkSuspendBrewingStepRegistryClient(
            URI.create(properties.registry.baseUrl),
            httpClient = suspendJdkHttpClient,
            modelCallMetrics = modelCallMetrics
        )

    private fun DemoApplicationProperties.etaModelBaseUrl(): String =
        model.etaBaseUrl.ifBlank { model.baseUrl }

    private fun DemoApplicationProperties.flavourModelBaseUrl(): String =
        model.flavourBaseUrl.ifBlank { model.baseUrl }
}
