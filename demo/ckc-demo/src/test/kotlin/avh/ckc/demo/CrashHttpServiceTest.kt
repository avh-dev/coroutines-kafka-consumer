package avh.ckc.demo

import avh.ckc.demo.internal.AuditLogFlusher
import avh.ckc.demo.internal.JvmHalter
import com.linecorp.armeria.client.WebClient
import com.linecorp.armeria.common.HttpMethod
import com.linecorp.armeria.common.HttpStatus
import com.linecorp.armeria.common.RequestHeaders
import com.linecorp.armeria.server.Server
import org.junit.jupiter.api.Test
import org.mockito.Mockito.verify
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.bean.override.mockito.MockitoBean
import kotlin.test.assertEquals

@SpringBootTest(
    properties = [
        "demo.kafka.enabled=false",
        "SERVER_PORT=0"
    ]
)
@AutoConfigureObservability
@ActiveProfiles("ckc")
class CrashHttpServiceTest {
    @Autowired
    private lateinit var server: Server

    @MockitoBean
    private lateinit var auditLogFlusher: AuditLogFlusher

    @MockitoBean
    private lateinit var jvmHalter: JvmHalter

    @Test
    fun `crash endpoint flushes audit logger before halting JVM`() {
        val response = webClient().execute(
            RequestHeaders.of(HttpMethod.POST, "/internal/crash")
        ).aggregate().join()

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.status())
        verify(auditLogFlusher).flushAndStop()
        verify(jvmHalter).halt(137)
    }

    private fun webClient(): WebClient =
        WebClient.of("http://127.0.0.1:${server.activeLocalPort()}")
}
