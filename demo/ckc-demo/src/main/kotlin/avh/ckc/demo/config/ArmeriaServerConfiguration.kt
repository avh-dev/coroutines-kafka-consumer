package avh.ckc.demo.config

import avh.ckc.demo.api.OrderHttpService
import com.linecorp.armeria.common.HttpResponse
import com.linecorp.armeria.common.MediaType
import com.linecorp.armeria.spring.ArmeriaServerConfigurator
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.springframework.beans.factory.ObjectProvider
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration(proxyBeanMethods = false)
class ArmeriaServerConfiguration {
    @Bean
    fun armeriaServerConfigurator(
        prometheusMeterRegistry: ObjectProvider<PrometheusMeterRegistry>,
        orderHttpService: ObjectProvider<OrderHttpService>
    ): ArmeriaServerConfigurator =
        ArmeriaServerConfigurator { serverBuilder ->
            orderHttpService.ifAvailable?.let(serverBuilder::annotatedService)
            serverBuilder.service("/actuator/prometheus") { _, _ ->
                HttpResponse.of(MediaType.PLAIN_TEXT_UTF_8, prometheusMeterRegistry.ifAvailable?.scrape().orEmpty())
            }
        }
}
