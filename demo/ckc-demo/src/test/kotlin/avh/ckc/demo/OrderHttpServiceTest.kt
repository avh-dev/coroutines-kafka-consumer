package avh.ckc.demo

import avh.ckc.demo.model.Batch
import avh.ckc.demo.model.Order
import avh.ckc.demo.repository.SyncBrewingStateRepository
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.linecorp.armeria.client.WebClient
import com.linecorp.armeria.common.HttpStatus
import com.linecorp.armeria.server.Server
import java.time.Duration
import org.junit.jupiter.api.Test
import org.mockito.BDDMockito.given
import org.springframework.boot.actuate.endpoint.web.WebEndpointsSupplier
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.bean.override.mockito.MockitoBean
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@SpringBootTest(
    properties = [
        "demo.kafka.enabled=false",
        "SERVER_PORT=0",
        "thread-stats.sampling-interval=10ms"
    ]
)
@AutoConfigureObservability
@ActiveProfiles("api")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OrderHttpServiceTest {
    @Autowired
    private lateinit var server: Server

    @Autowired
    private lateinit var webEndpointsSupplier: WebEndpointsSupplier

    private val objectMapper = jacksonObjectMapper()

    @MockitoBean
    private lateinit var brewingStateRepository: SyncBrewingStateRepository

    @Test
    fun `returns order with batch details`() {
        given(brewingStateRepository.findOrder("ord-7421")).willReturn(
            Order(
                orderId = "ord-7421",
                batchId = "batch-11",
                potionId = "healing-elixir",
                recipeId = "healing-elixir-v2",
                customerId = "guild-17",
                status = "BATCH_ASSIGNED",
                updatedAt = "2026-03-26T09:10:11Z"
            )
        )
        given(brewingStateRepository.findBatch("batch-11")).willReturn(
            Batch(
                batchId = "batch-11",
                recipeId = "healing-elixir-v2",
                potionId = "healing-elixir",
                cauldronId = "cauldron-3",
                status = "BREWING",
                orderIds = listOf("ord-7421", "ord-7422"),
                updatedAt = "2026-03-26T09:10:11Z"
            )
        )

        val response = webClient().get("/api/orders/ord-7421").aggregate().join()
        val json = objectMapper.readTree(response.contentUtf8())

        assertEquals(HttpStatus.OK, response.status())
        assertEquals("ord-7421", json.at("/order/orderId").textValue())
        assertEquals("BATCH_ASSIGNED", json.at("/order/status").textValue())
        assertEquals("batch-11", json.at("/batch/batchId").textValue())
        assertEquals("ord-7422", json.at("/batch/orderIds/1").textValue())
    }

    @Test
    fun `returns not found for missing order`() {
        given(brewingStateRepository.findOrder("missing")).willReturn(null)

        val response = webClient().get("/api/orders/missing").aggregate().join()

        assertEquals(HttpStatus.NOT_FOUND, response.status())
    }

    @Test
    fun `serves actuator health and prometheus endpoints`() {
        val endpointIds = webEndpointsSupplier.endpoints.map { it.endpointId.toString() }.toSet()
        assertTrue(endpointIds.contains("health"))
        assertTrue(endpointIds.contains("prometheus"))

        val client = webClient()
        assertTrue(client.get("/actuator/health").aggregate().join().status() != HttpStatus.NOT_FOUND)
        val prometheusResponse = client.get("/actuator/prometheus").aggregate().join()
        assertEquals(HttpStatus.OK, prometheusResponse.status())
        assertTrue(prometheusResponse.contentUtf8().contains("jvm_threads_live_threads"))
    }

    @Test
    fun `serves thread stats actuator latest grouped interval endpoint`() {
        val endpointIds = webEndpointsSupplier.endpoints.map { it.endpointId.toString() }.toSet()
        assertTrue(endpointIds.contains("threadstats"))

        val json = waitForThreadStatsGroups()

        assertEquals(true, json.at("/available").booleanValue())
        assertTrue(json.at("/stats/groups").isArray)
    }

    private fun webClient(): WebClient =
        WebClient.of("http://127.0.0.1:${server.activeLocalPort()}")

    private fun waitForThreadStatsGroups(): JsonNode {
        val deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos()
        var lastResponse = ""

        while (System.nanoTime() < deadline) {
            val response = webClient().get("/actuator/threadstats/groups").aggregate().join()
            assertEquals(HttpStatus.OK, response.status())

            lastResponse = response.contentUtf8()
            val json = objectMapper.readTree(lastResponse)
            if (json.at("/available").asBoolean(false)) {
                return json
            }

            Thread.sleep(25)
        }

        throw AssertionError("Thread Stats latest grouped interval did not become available. Last response: $lastResponse")
    }
}
