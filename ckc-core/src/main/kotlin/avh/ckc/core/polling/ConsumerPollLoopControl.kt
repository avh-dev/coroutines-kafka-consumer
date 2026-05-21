package avh.ckc.core.polling

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job

internal interface ConsumerPollLoopControl {
    fun start(): Job

    fun prepareForShutdown(): Deferred<Unit>
}
