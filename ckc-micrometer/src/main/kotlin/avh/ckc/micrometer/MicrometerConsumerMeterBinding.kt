package avh.ckc.micrometer

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import java.util.concurrent.atomic.AtomicInteger

/** Registers extension meters with the prefix and consumer tags of an existing CKC binding. */
interface MicrometerConsumerMeterBinding {
    fun extensionCounter(suffix: String, vararg tags: Pair<String, String>): Counter
    fun extensionTimer(suffix: String, vararg tags: Pair<String, String>): Timer

    /** Returns the value observed by this binding's gauge, registering it on first access. */
    fun extensionGaugeValue(suffix: String): AtomicInteger
}
