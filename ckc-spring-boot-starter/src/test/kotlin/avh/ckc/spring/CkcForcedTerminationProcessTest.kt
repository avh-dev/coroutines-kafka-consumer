package avh.ckc.spring

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File
import java.net.URLClassLoader
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class CkcForcedTerminationProcessTest {
    @Test
    fun `when child JVM shutdown hook is stuck then forced halt terminates process`() {
        val java = File(System.getProperty("java.home"), "bin/java").absolutePath
        val process = ProcessBuilder(
            java,
            "-cp",
            runtimeClasspath(),
            CkcHangingShutdownHookProbe::class.java.name
        ).redirectErrorStream(true).start()

        val completed = process.waitFor(5, TimeUnit.SECONDS)
        if (!completed) process.destroyForcibly()
        val output = process.inputStream.bufferedReader().readText()

        assertTrue(completed, "child JVM did not terminate; output=$output")
        assertTrue(output.contains("CKC_FORCE_EXIT_PROBE_READY"), output)
        assertEquals(1, process.exitValue(), output)
    }

    private fun runtimeClasspath(): String {
        val entries = System.getProperty("java.class.path")
            .split(File.pathSeparator)
            .toMutableSet()
        var loader: ClassLoader? = javaClass.classLoader
        while (loader != null) {
            if (loader is URLClassLoader) {
                loader.getURLs()
                    .filter { it.protocol == "file" }
                    .mapTo(entries) { File(it.toURI()).absolutePath }
            }
            loader = loader.parent
        }
        return entries.joinToString(File.pathSeparator)
    }
}

internal object CkcHangingShutdownHookProbe {
    @JvmStatic
    fun main(args: Array<String>) {
        Runtime.getRuntime().addShutdownHook(Thread {
            CountDownLatch(1).await()
        })
        println("CKC_FORCE_EXIT_PROBE_READY")
        CkcTerminalFailureCoordinator(
            closeContext = {},
            exitHandler = CkcTerminalFailureExitHandler { System.exit(it) },
            haltHandler = CkcTerminalFailureHaltHandler { Runtime.getRuntime().halt(it) },
            closeTimeoutMillis = 10L,
            exitTimeoutMillis = 100L
        ).trigger()
        Thread.sleep(10_000L)
    }
}
