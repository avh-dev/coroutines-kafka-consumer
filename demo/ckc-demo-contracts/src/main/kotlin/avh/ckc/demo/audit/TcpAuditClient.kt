package avh.ckc.demo.audit

import java.io.Closeable
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicReference

interface AuditLineWriter : Closeable {
    fun write(line: String)
}

class LazyAuditLineWriter(
    private val factory: () -> AuditLineWriter
) : AuditLineWriter {
    @Volatile
    private var delegate: AuditLineWriter? = null

    override fun write(line: String) {
        writer().write(line)
    }

    override fun close() {
        synchronized(this) {
            delegate?.close()
            delegate = null
        }
    }

    private fun writer(): AuditLineWriter {
        delegate?.let { return it }
        return synchronized(this) {
            delegate ?: factory().also { delegate = it }
        }
    }
}

class TcpAuditClient(
    host: String,
    port: Int,
    connectTimeoutMs: Int = 5_000
) : AuditLineWriter {
    private val failure = AtomicReference<Throwable?>()
    private val socket = Socket().apply {
        tcpNoDelay = true
        keepAlive = true
        connect(InetSocketAddress(host, port), connectTimeoutMs)
    }
    private val output = socket.getOutputStream()

    @Synchronized
    override fun write(line: String) {
        failure.get()?.let { throw IllegalStateException("TCP audit write failed", it) }
        try {
            output.write(encodeMessageEnvelope(line).toByteArray(UTF_8))
            output.write('\n'.code)
        } catch (error: Throwable) {
            failure.compareAndSet(null, error)
            throw IllegalStateException("TCP audit write failed", error)
        }
    }

    override fun close() {
        var closeError: Throwable? = null
        try {
            output.close()
        } catch (error: Throwable) {
            closeError = error
        }
        try {
            socket.close()
        } catch (error: Throwable) {
            if (closeError == null) {
                closeError = error
            }
        }
        closeError?.let { throw IllegalStateException("TCP audit close failed", it) }
    }
}

private fun encodeMessageEnvelope(line: String): String =
    buildString(line.length + 16) {
        append("{\"message\":\"")
        line.forEach { char ->
            when (char) {
                '\\' -> append("\\\\")
                '"' -> append("\\\"")
                '\b' -> append("\\b")
                '\u000C' -> append("\\f")
                '\n' -> append("\\n")
                '\r' -> append("\\r")
                '\t' -> append("\\t")
                else -> {
                    if (char.code < 0x20) {
                        append("\\u")
                        append(char.code.toString(16).padStart(4, '0'))
                    } else {
                        append(char)
                    }
                }
            }
        }
        append("\"}")
    }
