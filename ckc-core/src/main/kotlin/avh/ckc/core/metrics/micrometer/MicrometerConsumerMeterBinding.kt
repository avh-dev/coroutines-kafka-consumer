package avh.ckc.core.metrics.micrometer

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import java.util.concurrent.atomic.AtomicInteger

/**
 * Registers extension meters with the prefix, registry, and consumer tags of an existing CKC
 * metrics binding. Use bounded tag values. Timing measurements use the units supplied to [Timer].
 */
interface MicrometerConsumerMeterBinding {
    /** Registers or reuses a counter by suffix and additional tags. */
    fun extensionCounter(suffix: String, vararg tags: Pair<String, String>): Counter
    /** Registers or reuses a timer by suffix and additional tags. */
    fun extensionTimer(suffix: String, vararg tags: Pair<String, String>): Timer

    /** Returns the shared writable value sampled by the gauge, creating the meter on first access. */
    fun extensionGaugeValue(suffix: String): AtomicInteger
}
