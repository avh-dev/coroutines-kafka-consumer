package avh.ckc.core.processing

internal interface RecordProcessingLifecycle {
    fun start(onFailure: (Throwable) -> Unit)

    fun close(cause: Throwable?)

    suspend fun stop()
}
