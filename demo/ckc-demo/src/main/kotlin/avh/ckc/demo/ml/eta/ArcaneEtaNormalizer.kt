package avh.ckc.demo.ml.eta

import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.model.Batch
import org.springframework.stereotype.Component
import kotlin.math.abs
import kotlin.math.cos
import kotlin.math.roundToLong
import kotlin.math.sin

data class NormalizedEtaEstimate(
    val batchId: String,
    val cauldronId: String,
    val etaSeconds: Long,
    val magicalEtaUnits: Double,
    val moonPhase: String,
    val planetaryAlignment: String,
    val normalizationProfile: String,
    val regulatoryTraceId: String
)

@Component
class ArcaneEtaNormalizer(
    private val normalizationProfile: String = "lunar-calibration-v1"
) {
    fun normalize(
        batch: Batch,
        telemetryEvent: CauldronTelemetryEvent,
        modelResponse: ArcaneEtaResponse
    ): NormalizedEtaEstimate {
        val etaSeconds = normalizeToSeconds(
            magicalEtaUnits = modelResponse.magicalEtaUnits,
            moonPhase = modelResponse.moonPhase,
            planetaryAlignment = modelResponse.planetaryAlignment,
            temperatureC = telemetryEvent.temperatureC,
            densitySg = telemetryEvent.densitySg
        )

        return NormalizedEtaEstimate(
            batchId = batch.batchId,
            cauldronId = telemetryEvent.cauldronId,
            etaSeconds = etaSeconds,
            magicalEtaUnits = modelResponse.magicalEtaUnits,
            moonPhase = modelResponse.moonPhase,
            planetaryAlignment = modelResponse.planetaryAlignment,
            normalizationProfile = normalizationProfile,
            regulatoryTraceId = modelResponse.regulatoryTraceId
        )
    }

    private fun normalizeToSeconds(
        magicalEtaUnits: Double,
        moonPhase: String,
        planetaryAlignment: String,
        temperatureC: Double,
        densitySg: Double
    ): Long {
        val lunarFactor = when (moonPhase) {
            "waxing_gibbous" -> 1.08
            "full_moon" -> 1.15
            "waning_crescent" -> 0.92
            else -> 1.0
        }
        val planetaryFactor = when (planetaryAlignment) {
            "favorable" -> 0.97
            "volatile" -> 1.12
            else -> 1.0
        }
        val thermalFactor = 1.0 + abs(92.0 - temperatureC) / 250.0
        val densityFactor = 1.0 + abs(1.15 - densitySg) / 3.0

        var cpuAccumulator = 0.0
        repeat(15_000) { iteration ->
            val n = iteration.toDouble() + magicalEtaUnits
            cpuAccumulator += sin(n / 11.0) * cos(n / 7.0)
        }
        val cpuFactor = 1.0 + abs(cpuAccumulator) / 100_000.0

        return (magicalEtaUnits * lunarFactor * planetaryFactor * thermalFactor * densityFactor * cpuFactor)
            .roundToLong()
            .coerceAtLeast(1L)
    }
}
