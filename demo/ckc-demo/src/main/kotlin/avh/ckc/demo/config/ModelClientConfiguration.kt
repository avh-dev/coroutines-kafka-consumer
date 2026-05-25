package avh.ckc.demo.config

import avh.ckc.demo.ml.eta.JdkSyncArcaneEtaModelClient
import avh.ckc.demo.ml.eta.KtorSuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SyncArcaneEtaModelClient
import avh.ckc.demo.ml.flavour.JdkSyncOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.KtorSuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SyncOrderFlavourModelClient
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.net.URI
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

@Configuration(proxyBeanMethods = false)
class ModelClientConfiguration {
    @Bean
    @Profile("spring-kafka", "confluent-parallel")
    fun syncArcaneEtaModelClient(properties: DemoApplicationProperties): SyncArcaneEtaModelClient =
        JdkSyncArcaneEtaModelClient(URI.create(properties.model.baseUrl))

    @Bean(destroyMethod = "close")
    @Profile("ckc")
    fun suspendModelHttpDispatcher(): ExecutorCoroutineDispatcher {
        val threadCounter = AtomicInteger(1)
        return Executors.newFixedThreadPool(2) { runnable ->
            Thread(runnable, "ckc-demo-model-http-${threadCounter.getAndIncrement()}").apply {
                isDaemon = true
            }
        }.asCoroutineDispatcher()
    }

    @Bean(destroyMethod = "close")
    @Profile("ckc")
    fun suspendModelHttpClient(suspendModelHttpDispatcher: ExecutorCoroutineDispatcher): HttpClient =
        HttpClient(CIO) {
            engine {
                dispatcher = suspendModelHttpDispatcher
            }
        }

    @Bean
    @Profile("ckc")
    fun suspendArcaneEtaModelClient(
        properties: DemoApplicationProperties,
        suspendModelHttpClient: HttpClient,
        suspendModelHttpDispatcher: ExecutorCoroutineDispatcher
    ): SuspendArcaneEtaModelClient =
        KtorSuspendArcaneEtaModelClient(
            URI.create(properties.model.baseUrl),
            suspendModelHttpClient,
            suspendModelHttpDispatcher
        )

    @Bean
    @Profile("spring-kafka", "confluent-parallel")
    fun syncOrderFlavourModelClient(properties: DemoApplicationProperties): SyncOrderFlavourModelClient =
        JdkSyncOrderFlavourModelClient(URI.create(properties.model.baseUrl))

    @Bean
    @Profile("ckc")
    fun suspendOrderFlavourModelClient(
        properties: DemoApplicationProperties,
        suspendModelHttpClient: HttpClient,
        suspendModelHttpDispatcher: ExecutorCoroutineDispatcher
    ): SuspendOrderFlavourModelClient =
        KtorSuspendOrderFlavourModelClient(
            URI.create(properties.model.baseUrl),
            suspendModelHttpClient,
            suspendModelHttpDispatcher
        )
}
