package avh.ckc.demo.config

import avh.ckc.demo.api.OrderHttpService
import avh.ckc.demo.internal.CrashHttpService
import com.linecorp.armeria.spring.ArmeriaServerConfigurator
import org.springframework.beans.factory.ObjectProvider
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration(proxyBeanMethods = false)
class ArmeriaServerConfiguration {
    @Bean
    fun armeriaServerConfigurator(
        orderHttpService: ObjectProvider<OrderHttpService>,
        crashHttpService: CrashHttpService
    ): ArmeriaServerConfigurator =
        ArmeriaServerConfigurator { serverBuilder ->
            orderHttpService.ifAvailable?.let(serverBuilder::annotatedService)
            serverBuilder.annotatedService(crashHttpService)
        }
}
