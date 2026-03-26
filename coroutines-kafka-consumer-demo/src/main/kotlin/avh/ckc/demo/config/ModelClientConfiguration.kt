package avh.ckc.demo.config

import avh.ckc.demo.model.ArcaneEtaModelClient
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.net.URI

@Configuration(proxyBeanMethods = false)
class ModelClientConfiguration {
    @Bean
    fun arcaneEtaModelClient(properties: DemoApplicationProperties): ArcaneEtaModelClient =
        ArcaneEtaModelClient(URI.create(properties.model.baseUrl))
}
