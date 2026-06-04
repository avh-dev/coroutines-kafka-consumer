package avh.ckc.demo.internal

import com.linecorp.armeria.common.HttpResponse
import com.linecorp.armeria.common.HttpStatus
import com.linecorp.armeria.server.annotation.Blocking
import com.linecorp.armeria.server.annotation.Post
import com.linecorp.armeria.server.annotation.PathPrefix
import org.springframework.stereotype.Component

@Component
@PathPrefix("/internal")
class CrashHttpService(
    private val auditLogFlusher: AuditLogFlusher,
    private val jvmHalter: JvmHalter
) {
    @Blocking
    @Post("/crash")
    fun crash(): HttpResponse {
        auditLogFlusher.flushAndStop()
        jvmHalter.halt(137)
        return HttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR)
    }
}
