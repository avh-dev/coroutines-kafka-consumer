package avh.ckc.demo.config

import avh.ckc.demo.model.JdkSuspendArcaneEtaModelClient
import avh.ckc.demo.model.JdkSyncArcaneEtaModelClient
import avh.ckc.demo.model.SuspendArcaneEtaModelClient
import avh.ckc.demo.model.SyncArcaneEtaModelClient
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
}
