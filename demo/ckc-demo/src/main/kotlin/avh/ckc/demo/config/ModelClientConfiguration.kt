package avh.ckc.demo.config

import avh.ckc.demo.modelclient.eta.JdkSuspendArcaneEtaModelClient
import avh.ckc.demo.modelclient.eta.JdkSyncArcaneEtaModelClient
import avh.ckc.demo.modelclient.eta.SuspendArcaneEtaModelClient
import avh.ckc.demo.modelclient.eta.SyncArcaneEtaModelClient
import avh.ckc.demo.modelclient.flavour.JdkSuspendOrderFlavourModelClient
import avh.ckc.demo.modelclient.flavour.JdkSyncOrderFlavourModelClient
import avh.ckc.demo.modelclient.flavour.SuspendOrderFlavourModelClient
import avh.ckc.demo.modelclient.flavour.SyncOrderFlavourModelClient
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.net.URI

@Configuration(proxyBeanMethods = false)
class ModelClientConfiguration {
    @Bean
    fun syncArcaneEtaModelClient(properties: DemoApplicationProperties): SyncArcaneEtaModelClient =
        JdkSyncArcaneEtaModelClient(URI.create(properties.model.baseUrl))

    @Bean
    fun suspendArcaneEtaModelClient(properties: DemoApplicationProperties): SuspendArcaneEtaModelClient =
        JdkSuspendArcaneEtaModelClient(URI.create(properties.model.baseUrl))

    @Bean
    fun syncOrderFlavourModelClient(properties: DemoApplicationProperties): SyncOrderFlavourModelClient =
        JdkSyncOrderFlavourModelClient(URI.create(properties.model.baseUrl))

    @Bean
    fun suspendOrderFlavourModelClient(properties: DemoApplicationProperties): SuspendOrderFlavourModelClient =
        JdkSuspendOrderFlavourModelClient(URI.create(properties.model.baseUrl))
}
