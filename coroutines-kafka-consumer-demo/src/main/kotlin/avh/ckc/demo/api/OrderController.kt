package avh.ckc.demo.api

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.concurrent.CompletionStage

@RestController
@RequestMapping("/api/orders")
class OrderController(
    private val orderQueryService: OrderQueryService
) {
    @GetMapping("/{orderId}")
    fun getOrder(@PathVariable orderId: String): CompletionStage<ResponseEntity<OrderTrackingResponse>> =
        orderQueryService.findOrder(orderId)
            .thenApply { response ->
                if (response == null) {
                    ResponseEntity.notFound().build()
                } else {
                    ResponseEntity.ok(response)
                }
            }
}
