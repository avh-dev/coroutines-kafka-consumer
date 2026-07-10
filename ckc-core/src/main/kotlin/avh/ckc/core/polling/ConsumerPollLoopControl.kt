package avh.ckc.core.polling

import avh.ckc.core.PollLoopStateSnapshot
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job

internal interface ConsumerPollLoopControl {
    fun start(): Job

    fun prepareForShutdown(): Deferred<Unit>

    fun stateSnapshot(): PollLoopStateSnapshot
}
