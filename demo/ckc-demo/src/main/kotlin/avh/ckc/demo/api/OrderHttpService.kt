package avh.ckc.demo.api

import com.linecorp.armeria.common.HttpResponse
import com.linecorp.armeria.common.HttpStatus
import com.linecorp.armeria.server.annotation.Blocking
import com.linecorp.armeria.server.annotation.Get
import com.linecorp.armeria.server.annotation.Param
import com.linecorp.armeria.server.annotation.PathPrefix
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

@Component
@Profile("api")
@PathPrefix("/api/orders")
class OrderHttpService(
    private val orderQueryService: OrderQueryService
) {
    @Blocking
    @Get("/{orderId}")
    fun getOrder(@Param("orderId") orderId: String): HttpResponse =
        orderQueryService.findOrder(orderId)?.let { response ->
            HttpResponse.ofJson(response)
        } ?: HttpResponse.of(HttpStatus.NOT_FOUND)
}
