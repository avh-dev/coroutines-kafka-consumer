package avh.ckc.demo.config

import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.actuate.endpoint.EndpointAccessResolver
import org.springframework.boot.actuate.endpoint.EndpointFilter
import org.springframework.boot.actuate.endpoint.OperationFilter
import org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointProperties
import org.springframework.boot.actuate.endpoint.invoke.OperationInvokerAdvisor
import org.springframework.boot.actuate.endpoint.invoke.ParameterValueMapper
import org.springframework.boot.actuate.endpoint.web.AdditionalPathsMapper
import org.springframework.boot.actuate.endpoint.web.EndpointMediaTypes
import org.springframework.boot.actuate.endpoint.web.ExposableWebEndpoint
import org.springframework.boot.actuate.endpoint.web.PathMapper
import org.springframework.boot.actuate.endpoint.web.WebEndpointsSupplier
import org.springframework.boot.actuate.endpoint.web.WebOperation
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpointDiscoverer
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(WebEndpointProperties::class)
class ArmeriaActuatorEndpointConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun webEndpointPathMapper(properties: WebEndpointProperties): PathMapper =
        PathMapper { endpointId ->
            properties.pathMapping[endpointId.toString()] ?: endpointId.toString()
        }

    @Bean
    @ConditionalOnMissingBean
    fun endpointMediaTypes(): EndpointMediaTypes =
        EndpointMediaTypes.DEFAULT

    @Bean
    @ConditionalOnMissingBean(name = ["webAccessPropertiesOperationFilter"])
    fun webAccessPropertiesOperationFilter(endpointAccessResolver: EndpointAccessResolver): OperationFilter<WebOperation> =
        OperationFilter.byAccess(endpointAccessResolver)

    @Bean
    @ConditionalOnMissingBean(WebEndpointsSupplier::class)
    fun webEndpointDiscoverer(
        applicationContext: ApplicationContext,
        parameterValueMapper: ParameterValueMapper,
        endpointMediaTypes: EndpointMediaTypes,
        endpointPathMappers: ObjectProvider<PathMapper>,
        additionalPathsMappers: ObjectProvider<AdditionalPathsMapper>,
        invokerAdvisors: ObjectProvider<OperationInvokerAdvisor>,
        endpointFilters: ObjectProvider<EndpointFilter<ExposableWebEndpoint>>,
        operationFilters: ObjectProvider<OperationFilter<WebOperation>>
    ): WebEndpointDiscoverer =
        WebEndpointDiscoverer(
            applicationContext,
            parameterValueMapper,
            endpointMediaTypes,
            endpointPathMappers.orderedStream().toList(),
            additionalPathsMappers.orderedStream().toList(),
            invokerAdvisors.orderedStream().toList(),
            endpointFilters.orderedStream().toList(),
            operationFilters.orderedStream().toList()
        )
}
