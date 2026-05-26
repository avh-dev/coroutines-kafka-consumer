package avh.ckc.demo

import avh.ckc.demo.ml.eta.ArmeriaSuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SyncArcaneEtaModelClient
import avh.ckc.demo.ml.flavour.ArmeriaSuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SyncOrderFlavourModelClient
import com.linecorp.armeria.client.WebClient
import io.ktor.client.HttpClient
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.test.context.ActiveProfiles
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

@SpringBootTest(
    properties = [
        "demo.kafka.enabled=false",
        "demo.model.client=ARMERIA"
    ]
)
@ActiveProfiles("ckc")
class CkcArmeriaModelClientContextTest(
    @Autowired private val applicationContext: ApplicationContext
) {
    @Test
    fun `ckc profile can use armeria suspend model clients`() {
        assertTrue(applicationContext.getBeansOfType(WebClient::class.java).isNotEmpty())
        assertFalse(applicationContext.getBeansOfType(HttpClient::class.java).isNotEmpty())
        assertFalse(applicationContext.getBeansOfType(SyncArcaneEtaModelClient::class.java).isNotEmpty())
        assertFalse(applicationContext.getBeansOfType(SyncOrderFlavourModelClient::class.java).isNotEmpty())
        assertIs<ArmeriaSuspendArcaneEtaModelClient>(
            applicationContext.getBean(SuspendArcaneEtaModelClient::class.java)
        )
        assertIs<ArmeriaSuspendOrderFlavourModelClient>(
            applicationContext.getBean(SuspendOrderFlavourModelClient::class.java)
        )
    }
}
