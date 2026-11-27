package avh.ckc.spring

import avh.ckc.warmup.TrafficWarmupForwardedEvent
import avh.ckc.warmup.TrafficWarmupLifecycleEvent

/** Spring event carrying a transition from a named warm-up context. */
data class CkcWarmupLifecycleEvent(
    val contextName: String,
    val event: TrafficWarmupLifecycleEvent
)

/** Spring event carrying a confirmed append by a named donor consumer. */
data class CkcWarmupForwardedEvent(
    val contextName: String,
    val consumerName: String,
    val event: TrafficWarmupForwardedEvent
)
