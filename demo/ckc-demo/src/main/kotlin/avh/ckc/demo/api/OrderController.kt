package avh.ckc.demo.api

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/orders")
class OrderController(
    private val orderQueryService: OrderQueryService
) {
    @GetMapping("/{orderId}")
    fun getOrder(@PathVariable orderId: String): ResponseEntity<OrderTrackingResponse> {
        val response = orderQueryService.findOrder(orderId)
        return if (response == null) {
            ResponseEntity.notFound().build()
        } else {
            ResponseEntity.ok(response)
        }
    }
}
