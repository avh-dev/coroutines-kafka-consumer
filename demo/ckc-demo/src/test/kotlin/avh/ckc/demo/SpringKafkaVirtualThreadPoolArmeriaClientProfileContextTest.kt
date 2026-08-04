package avh.ckc.demo

import avh.ckc.demo.ml.eta.ArmeriaSyncArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SyncArcaneEtaModelClient
import avh.ckc.demo.ml.flavour.ArmeriaSyncOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SyncOrderFlavourModelClient
import avh.ckc.demo.registry.ArmeriaSyncBrewingStepRegistryClient
import avh.ckc.demo.registry.SyncBrewingStepRegistryClient
import com.linecorp.armeria.client.WebClient
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.test.context.ActiveProfiles
import java.net.http.HttpClient
import kotlin.test.assertIs
import kotlin.test.assertTrue

@SpringBootTest(
    properties = [
        "demo.kafka.enabled=false",
        "demo.model.sync-http-client=armeria",
        "SERVER_PORT=0",
        "spring.autoconfigure.exclude=com.linecorp.armeria.spring.ArmeriaAutoConfiguration," +
                "com.linecorp.armeria.spring.actuate.ArmeriaSpringActuatorAutoConfiguration"
    ]
)
@ActiveProfiles("spring-kafka-virtual-thread-pool")
class SpringKafkaVirtualThreadPoolArmeriaClientProfileContextTest(
    @Autowired private val applicationContext: ApplicationContext
) {
    @Test
    fun `virtual thread profile selects sync Armeria clients without JDK client`() {
        assertIs<ArmeriaSyncArcaneEtaModelClient>(
            applicationContext.getBean(SyncArcaneEtaModelClient::class.java)
        )
        assertIs<ArmeriaSyncOrderFlavourModelClient>(
            applicationContext.getBean(SyncOrderFlavourModelClient::class.java)
        )
        assertIs<ArmeriaSyncBrewingStepRegistryClient>(
            applicationContext.getBean(SyncBrewingStepRegistryClient::class.java)
        )
        assertTrue(applicationContext.getBeansOfType(WebClient::class.java).isNotEmpty())
        assertTrue(applicationContext.getBeansOfType(HttpClient::class.java).isEmpty())
    }
}
