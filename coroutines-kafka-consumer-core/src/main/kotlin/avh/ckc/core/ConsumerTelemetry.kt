package avh.ckc.core

interface ConsumerTelemetry {
    fun onPoll(recordsCount: Int, durationNanos: Long) = Unit

    fun onRecordProcessed(topic: String, partition: Int, recordAgeMillis: Long, durationNanos: Long) = Unit

    fun onRecordFailed(topic: String, partition: Int, recordAgeMillis: Long, error: Throwable, durationNanos: Long) = Unit

    fun onRetry(topic: String, partition: Int, attempt: Int, error: Throwable) = Unit

    fun onCommit(partitionsCount: Int, durationNanos: Long, success: Boolean) = Unit

    fun onConsumerFailure(error: Throwable) = Unit

    companion object {
        val NOOP: ConsumerTelemetry = object : ConsumerTelemetry {}
    }
}
