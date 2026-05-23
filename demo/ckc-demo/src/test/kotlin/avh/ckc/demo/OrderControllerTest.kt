package avh.ckc.demo

import avh.ckc.demo.repository.BatchState
import avh.ckc.demo.repository.OrderState
import avh.ckc.demo.repository.SyncBrewingStateRepository
import org.junit.jupiter.api.Test
import org.mockito.BDDMockito.given
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

@SpringBootTest(
    properties = [
        "demo.kafka.enabled=false"
    ]
)
@AutoConfigureMockMvc
class OrderControllerTest {
    @Autowired
    private lateinit var mockMvc: MockMvc

    @MockBean
    private lateinit var brewingStateRepository: SyncBrewingStateRepository

    @Test
    fun `returns order with batch details`() {
        given(brewingStateRepository.findOrder("ord-7421")).willReturn(
            OrderState(
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
            BatchState(
                batchId = "batch-11",
                recipeId = "healing-elixir-v2",
                potionId = "healing-elixir",
                cauldronId = "cauldron-3",
                status = "BREWING",
                orderIds = listOf("ord-7421", "ord-7422"),
                updatedAt = "2026-03-26T09:10:11Z"
            )
        )

        mockMvc.perform(get("/api/orders/ord-7421"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.order.orderId").value("ord-7421"))
            .andExpect(jsonPath("$.order.status").value("BATCH_ASSIGNED"))
            .andExpect(jsonPath("$.batch.batchId").value("batch-11"))
            .andExpect(jsonPath("$.batch.orderIds[1]").value("ord-7422"))
    }

    @Test
    fun `returns not found for missing order`() {
        given(brewingStateRepository.findOrder("missing")).willReturn(null)

        mockMvc.perform(get("/api/orders/missing"))
            .andExpect(status().isNotFound)
    }
}
