package avh.ckc.loadtest.domain

import java.time.Instant
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class CauldronTelemetryFactoryTest {
    @Test
    fun `generates independently randomized diagnostics payloads of configured size`() {
        val factory = CauldronTelemetryFactory(diagnosticsBlobSize = 256, random = Random(42))
        val batch = activeBatch()

        val first = factory.create(batch, Instant.EPOCH).diagnosticsBlob.toByteArray()
        val second = factory.create(batch, Instant.EPOCH.plusSeconds(1)).diagnosticsBlob.toByteArray()

        assertEquals(256, first.size)
        assertEquals(256, second.size)
        assertFalse(first.contentEquals(second))
    }

    @Test
    fun `supports an empty diagnostics payload`() {
        val event = CauldronTelemetryFactory(diagnosticsBlobSize = 0, random = Random(42))
            .create(activeBatch(), Instant.EPOCH)

        assertContentEquals(byteArrayOf(), event.diagnosticsBlob.toByteArray())
    }

    private fun activeBatch() = ActiveBatch(
        batchId = "batch-1",
        cauldronId = "cauldron-1",
        potion = PotionRecipe("potion-1", "recipe-1"),
        orders = emptyList(),
        startedAt = Instant.EPOCH,
        completesAt = Instant.EPOCH.plusSeconds(60)
    )
}
