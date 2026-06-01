package avh.ckc.demo.config

import avh.ckc.demo.ml.ModelCallMetrics
import avh.ckc.demo.ml.eta.ArmeriaSuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.JdkSyncArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SyncArcaneEtaModelClient
import avh.ckc.demo.ml.flavour.ArmeriaSuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.JdkSyncOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SyncOrderFlavourModelClient
import com.linecorp.armeria.client.WebClient
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.net.URI

@Configuration(proxyBeanMethods = false)
class ModelClientConfiguration {
    @Bean
    fun modelCallMetrics(meterRegistry: MeterRegistry): ModelCallMetrics =
        ModelCallMetrics(meterRegistry)

    @Bean
    @Profile("spring-kafka", "confluent-parallel", "ckc-sync")
    fun syncArcaneEtaModelClient(
        properties: DemoApplicationProperties,
        modelCallMetrics: ModelCallMetrics
    ): SyncArcaneEtaModelClient =
        JdkSyncArcaneEtaModelClient(URI.create(properties.model.baseUrl), modelCallMetrics = modelCallMetrics)

    @Bean
    @Profile("ckc", "confluent-parallel-reactor")
    fun armeriaModelWebClient(properties: DemoApplicationProperties): WebClient =
        WebClient.of(properties.model.baseUrl)

    @Bean
    @Profile("ckc", "confluent-parallel-reactor")
    fun armeriaSuspendArcaneEtaModelClient(
        armeriaModelWebClient: WebClient,
        modelCallMetrics: ModelCallMetrics
    ): SuspendArcaneEtaModelClient =
        ArmeriaSuspendArcaneEtaModelClient(armeriaModelWebClient, modelCallMetrics = modelCallMetrics)

    @Bean
    @Profile("spring-kafka", "confluent-parallel", "ckc-sync")
    fun syncOrderFlavourModelClient(
        properties: DemoApplicationProperties,
        modelCallMetrics: ModelCallMetrics
    ): SyncOrderFlavourModelClient =
        JdkSyncOrderFlavourModelClient(URI.create(properties.model.baseUrl), modelCallMetrics = modelCallMetrics)

    @Bean
    @Profile("ckc", "confluent-parallel-reactor")
    fun armeriaSuspendOrderFlavourModelClient(
        armeriaModelWebClient: WebClient,
        modelCallMetrics: ModelCallMetrics
    ): SuspendOrderFlavourModelClient =
        ArmeriaSuspendOrderFlavourModelClient(armeriaModelWebClient, modelCallMetrics = modelCallMetrics)
}
