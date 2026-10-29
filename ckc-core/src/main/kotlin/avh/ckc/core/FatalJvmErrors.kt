package avh.ckc.core

/** JVM failures from which record-level retry or skip logic must not attempt to recover. */
@Suppress("DEPRECATION")
internal fun Throwable.isFatalJvmError(): Boolean =
    this is VirtualMachineError || this is ThreadDeath || this is LinkageError
