package avh.ckc.demostubs

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class DemoStubsSettingsStateTest {
    @Test
    fun `restores saved settings`() {
        val saved = settings(12)
        val state = DemoStubsSettingsState(FakeStore(saved))

        assertEquals(saved, state.get())
    }

    @Test
    fun `uses baseline when settings cannot be restored`() {
        val state = DemoStubsSettingsState(FakeStore(loadFailure = IllegalStateException("Redis unavailable")))

        assertEquals(DemoStubsSettings.baseline(), state.get())
    }

    @Test
    fun `persists updates before exposing them`() {
        val store = FakeStore()
        val state = DemoStubsSettingsState(store)
        val update = settings(5)

        state.update(update)

        assertEquals(update, store.saved)
        assertEquals(update, state.get())
    }

    @Test
    fun `keeps previous settings when persistence fails`() {
        val previous = settings(8)
        val state = DemoStubsSettingsState(FakeStore(previous, saveFailure = IllegalStateException("Redis unavailable")))

        assertFailsWith<IllegalStateException> {
            state.update(settings(4))
        }
        assertEquals(previous, state.get())
    }

    @Test
    fun `applies settings published by another pod`() {
        val store = FakeStore()
        val state = DemoStubsSettingsState(store)
        val update = settings(3)

        store.publish(update)

        assertEquals(update, state.get())
    }

    private fun settings(delayMs: Long): DemoStubsSettings =
        DemoStubsSettings(
            eta = ModelLatencySettings(delayMs, delayMs, delayMs, delayMs),
            flavour = ModelLatencySettings(delayMs, delayMs, delayMs, delayMs),
            errorRatePercent = 0
        )

    private class FakeStore(
        private val loaded: DemoStubsSettings? = null,
        private val loadFailure: Exception? = null,
        private val saveFailure: Exception? = null
    ) : DemoStubsSettingsStore {
        var saved: DemoStubsSettings? = null
            private set
        private var listener: ((DemoStubsSettings) -> Unit)? = null

        override fun load(): DemoStubsSettings? {
            loadFailure?.let { throw it }
            return loaded
        }

        override fun save(settings: DemoStubsSettings) {
            saveFailure?.let { throw it }
            saved = settings
        }

        override fun subscribe(listener: (DemoStubsSettings) -> Unit) {
            this.listener = listener
        }

        fun publish(settings: DemoStubsSettings) {
            listener?.invoke(settings)
        }

        override fun close() = Unit
    }
}
