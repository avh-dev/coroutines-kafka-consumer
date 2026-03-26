package avh.ckc.demo

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(
    properties = [
        "demo.kafka.enabled=false"
    ]
)
@ActiveProfiles("ckc")
class CkcProfileContextTest {
    @Test
    fun contextLoads() {
    }
}
