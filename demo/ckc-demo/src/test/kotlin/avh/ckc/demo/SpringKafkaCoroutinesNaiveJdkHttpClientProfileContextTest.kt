package avh.ckc.demo

import avh.ckc.demo.ml.eta.JdkSuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SuspendArcaneEtaModelClient
import avh.ckc.demo.ml.flavour.JdkSuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SuspendOrderFlavourModelClient
import avh.ckc.demo.registry.JdkSuspendBrewingStepRegistryClient
import avh.ckc.demo.registry.SuspendBrewingStepRegistryClient
import com.linecorp.armeria.client.WebClient
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.test.context.ActiveProfiles
import kotlin.test.assertIs
import kotlin.test.assertTrue

@SpringBootTest(
    properties = [
        "demo.kafka.enabled=false",
        "demo.model.http-client=jdk",
        "SERVER_PORT=0",
        "spring.autoconfigure.exclude=com.linecorp.armeria.spring.ArmeriaAutoConfiguration," +
                "com.linecorp.armeria.spring.actuate.ArmeriaSpringActuatorAutoConfiguration"
    ]
)
@ActiveProfiles("spring-kafka-coroutines-naive")
class SpringKafkaCoroutinesNaiveJdkHttpClientProfileContextTest(
    @Autowired private val applicationContext: ApplicationContext
) {
    @Test
    fun `naive profile selects JDK suspend clients without Armeria web clients`() {
        assertIs<JdkSuspendArcaneEtaModelClient>(
            applicationContext.getBean(SuspendArcaneEtaModelClient::class.java)
        )
        assertIs<JdkSuspendOrderFlavourModelClient>(
            applicationContext.getBean(SuspendOrderFlavourModelClient::class.java)
        )
        assertIs<JdkSuspendBrewingStepRegistryClient>(
            applicationContext.getBean(SuspendBrewingStepRegistryClient::class.java)
        )
        assertTrue(applicationContext.getBeansOfType(WebClient::class.java).isEmpty())
    }
}
