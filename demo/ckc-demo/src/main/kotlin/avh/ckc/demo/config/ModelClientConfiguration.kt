package avh.ckc.demo.config

import avh.ckc.demo.ml.ModelCallMetrics
import avh.ckc.demo.ml.eta.ArmeriaSyncArcaneEtaModelClient
import avh.ckc.demo.ml.eta.ArmeriaSuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.JdkSuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.JdkSyncArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SyncArcaneEtaModelClient
import avh.ckc.demo.ml.flavour.ArmeriaSyncOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.ArmeriaSuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.JdkSuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.JdkSyncOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SyncOrderFlavourModelClient
import avh.ckc.demo.registry.ArmeriaSyncBrewingStepRegistryClient
import avh.ckc.demo.registry.ArmeriaSuspendBrewingStepRegistryClient
import avh.ckc.demo.registry.JdkSuspendBrewingStepRegistryClient
import avh.ckc.demo.registry.JdkSyncBrewingStepRegistryClient
import avh.ckc.demo.registry.SuspendBrewingStepRegistryClient
import avh.ckc.demo.registry.SyncBrewingStepRegistryClient
import com.linecorp.armeria.client.WebClient
import com.linecorp.armeria.client.endpoint.EndpointSelectionStrategy
import com.linecorp.armeria.client.endpoint.dns.DnsAddressEndpointGroup
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
    @Bean(destroyMethod = "close")
    fun modelStubEndpointGroups(properties: DemoApplicationProperties): ModelStubEndpointGroups =
        ModelStubEndpointGroups(properties.model.distributeStubRequests)

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
    @ConditionalOnProperty(prefix = "demo.model", name = ["sync-http-client"], havingValue = "jdk")
    fun syncJdkHttpClient(
        @Qualifier("syncJdkHttpClientVirtualExecutor") virtualExecutorProvider: ObjectProvider<ExecutorService>
    ): HttpClient {
        val builder = HttpClient.newBuilder()
        virtualExecutorProvider.ifAvailable { executor -> builder.executor(executor) }
        return builder.build()
    }

    @Bean
    @Profile("spring-kafka", "spring-kafka-thread-pool", "spring-kafka-virtual-thread-pool", "confluent-parallel", "ckc-sync")
    @ConditionalOnProperty(prefix = "demo.model", name = ["sync-http-client"], havingValue = "jdk")
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
    @Profile("spring-kafka", "spring-kafka-thread-pool", "spring-kafka-virtual-thread-pool", "confluent-parallel", "ckc-sync")
    @ConditionalOnProperty(prefix = "demo.model", name = ["sync-http-client"], havingValue = "armeria", matchIfMissing = true)
    fun syncArmeriaEtaModelWebClient(properties: DemoApplicationProperties, modelStubEndpointGroups: ModelStubEndpointGroups): WebClient =
        modelStubEndpointGroups.client(properties.etaModelBaseUrl())

    @Bean
    @Profile("spring-kafka", "spring-kafka-thread-pool", "spring-kafka-virtual-thread-pool", "confluent-parallel", "ckc-sync")
    @ConditionalOnProperty(prefix = "demo.model", name = ["sync-http-client"], havingValue = "armeria", matchIfMissing = true)
    fun armeriaSyncArcaneEtaModelClient(
        @Qualifier("syncArmeriaEtaModelWebClient") syncArmeriaEtaModelWebClient: WebClient,
        modelCallMetrics: ModelCallMetrics
    ): SyncArcaneEtaModelClient =
        ArmeriaSyncArcaneEtaModelClient(syncArmeriaEtaModelWebClient, modelCallMetrics = modelCallMetrics)

    @Bean
    @Profile("ckc", "ckc-spring-boot", "confluent-parallel-reactor", "spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.model", name = ["http-client"], havingValue = "armeria", matchIfMissing = true)
    fun armeriaEtaModelWebClient(properties: DemoApplicationProperties, modelStubEndpointGroups: ModelStubEndpointGroups): WebClient =
        modelStubEndpointGroups.client(properties.etaModelBaseUrl())

    @Bean
    @Profile("ckc", "ckc-spring-boot", "confluent-parallel-reactor", "spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.model", name = ["http-client"], havingValue = "armeria", matchIfMissing = true)
    fun armeriaFlavourModelWebClient(properties: DemoApplicationProperties, modelStubEndpointGroups: ModelStubEndpointGroups): WebClient =
        modelStubEndpointGroups.client(properties.flavourModelBaseUrl())

    @Bean
    @Profile("ckc", "ckc-spring-boot", "confluent-parallel-reactor", "spring-kafka-coroutines-naive")
    @ConditionalOnProperty(prefix = "demo.model", name = ["http-client"], havingValue = "armeria", matchIfMissing = true)
    fun armeriaRegistryWebClient(properties: DemoApplicationProperties, modelStubEndpointGroups: ModelStubEndpointGroups): WebClient =
        modelStubEndpointGroups.client(properties.registry.baseUrl)

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
    @ConditionalOnProperty(prefix = "demo.model", name = ["sync-http-client"], havingValue = "jdk")
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
    @Profile("spring-kafka", "spring-kafka-thread-pool", "spring-kafka-virtual-thread-pool", "confluent-parallel", "ckc-sync")
    @ConditionalOnProperty(prefix = "demo.model", name = ["sync-http-client"], havingValue = "armeria", matchIfMissing = true)
    fun syncArmeriaFlavourModelWebClient(properties: DemoApplicationProperties, modelStubEndpointGroups: ModelStubEndpointGroups): WebClient =
        modelStubEndpointGroups.client(properties.flavourModelBaseUrl())

    @Bean
    @Profile("spring-kafka", "spring-kafka-thread-pool", "spring-kafka-virtual-thread-pool", "confluent-parallel", "ckc-sync")
    @ConditionalOnProperty(prefix = "demo.model", name = ["sync-http-client"], havingValue = "armeria", matchIfMissing = true)
    fun armeriaSyncOrderFlavourModelClient(
        @Qualifier("syncArmeriaFlavourModelWebClient") syncArmeriaFlavourModelWebClient: WebClient,
        modelCallMetrics: ModelCallMetrics
    ): SyncOrderFlavourModelClient =
        ArmeriaSyncOrderFlavourModelClient(syncArmeriaFlavourModelWebClient, modelCallMetrics = modelCallMetrics)

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
    @ConditionalOnProperty(prefix = "demo.model", name = ["sync-http-client"], havingValue = "jdk")
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
    @Profile("spring-kafka", "spring-kafka-thread-pool", "spring-kafka-virtual-thread-pool", "confluent-parallel", "ckc-sync")
    @ConditionalOnProperty(prefix = "demo.model", name = ["sync-http-client"], havingValue = "armeria", matchIfMissing = true)
    fun syncArmeriaRegistryWebClient(properties: DemoApplicationProperties, modelStubEndpointGroups: ModelStubEndpointGroups): WebClient =
        modelStubEndpointGroups.client(properties.registry.baseUrl)

    @Bean
    @Profile("spring-kafka", "spring-kafka-thread-pool", "spring-kafka-virtual-thread-pool", "confluent-parallel", "ckc-sync")
    @ConditionalOnProperty(prefix = "demo.model", name = ["sync-http-client"], havingValue = "armeria", matchIfMissing = true)
    fun armeriaSyncBrewingStepRegistryClient(
        @Qualifier("syncArmeriaRegistryWebClient") syncArmeriaRegistryWebClient: WebClient,
        modelCallMetrics: ModelCallMetrics
    ): SyncBrewingStepRegistryClient =
        ArmeriaSyncBrewingStepRegistryClient(syncArmeriaRegistryWebClient, modelCallMetrics = modelCallMetrics)

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

class ModelStubEndpointGroups(private val enabled: Boolean) : AutoCloseable {
    private val groups = mutableMapOf<Pair<String, Int>, DnsAddressEndpointGroup>()

    @Synchronized
    fun client(baseUrl: String): WebClient {
        if (!enabled) return WebClient.of(baseUrl)
        val uri = URI.create(baseUrl)
        require(uri.scheme == "http" && uri.host != null && uri.rawPath.isNullOrEmpty()) {
            "Pod-aware stub routing requires an HTTP origin URL: $baseUrl"
        }
        val port = if (uri.port == -1) 80 else uri.port
        val group = groups.getOrPut(uri.host to port) {
            DnsAddressEndpointGroup.builder(uri.host)
                .port(port)
                .selectionStrategy(EndpointSelectionStrategy.roundRobin())
                .build()
        }
        return WebClient.of("h2c", group)
    }

    @Synchronized
    override fun close() {
        groups.values.forEach { it.close() }
        groups.clear()
    }
}
