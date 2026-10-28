package avh.ckc.spring

/** Isolates process termination so starter applications can test terminal-failure policy. */
fun interface CkcTerminalFailureExitHandler {
    fun exit(status: Int)
}
