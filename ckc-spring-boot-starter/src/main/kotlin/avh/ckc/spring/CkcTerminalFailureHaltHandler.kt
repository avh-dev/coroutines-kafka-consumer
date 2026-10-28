package avh.ckc.spring

/** Isolates forced JVM termination used when normal shutdown hooks do not finish. */
fun interface CkcTerminalFailureHaltHandler {
    fun halt(status: Int)
}
