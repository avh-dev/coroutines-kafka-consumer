package avh.ckc.demostubs

import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.call
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.request.receiveText
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import kotlinx.coroutines.delay
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.abs
import kotlin.random.Random

fun main() {
    val config = DemoStubsConfig.fromEnvironment()
    val json = Json { ignoreUnknownKeys = true }
    val delaySampler = DelaySampler(Random.Default)
    val latencySettings = AtomicReference(
        ModelLatencyRegistry(
            eta = config.etaLatency,
            flavour = config.flavourLatency
        )
    )
    val server = embeddedServer(
        factory = Netty,
        port = config.port,
        host = "0.0.0.0",
        configure = {
            connectionGroupSize = maxOf(1, config.workers / 4)
            workerGroupSize = config.workers
            callGroupSize = config.workers
        }
    ) {
        routing {
            get("/health") {
                call.respondText("""{"status":"UP"}""", ContentType.Application.Json)
            }

            get("/latency") {
                call.respondText(
                    json.encodeToString(ModelLatencyRegistry.serializer(), latencySettings.get()),
                    ContentType.Application.Json
                )
            }

            post("/latency") {
                try {
                    val update = json.decodeFromString(ModelLatencyRegistry.serializer(), call.receiveText())
                    latencySettings.set(update)
                    call.respondText(
                        json.encodeToString(ModelLatencyRegistry.serializer(), update),
                        ContentType.Application.Json
                    )
                } catch (_: Exception) {
                    call.respondText(
                        """{"error":"bad request"}""",
                        ContentType.Application.Json,
                        HttpStatusCode.BadRequest
                    )
                }
            }

            post("/eta") {
                try {
                    val request = json.decodeFromString(ArcaneEtaRequest.serializer(), call.receiveText())
                    val shouldFail = Random.nextInt(100) < config.errorRatePercent
                    delay(delaySampler.sampleDelayMillis(latencySettings.get().eta))

                    if (shouldFail) {
                        call.respondText(
                            """{"error":"model temporarily unavailable","trace_id":"${UUID.randomUUID()}"}""",
                            ContentType.Application.Json,
                            HttpStatusCode.ServiceUnavailable
                        )
                        return@post
                    }

                    val response = estimate(request)
                    call.respondText(
                        json.encodeToString(ArcaneEtaResponse.serializer(), response),
                        ContentType.Application.Json
                    )
                } catch (_: Exception) {
                    call.respondText(
                        """{"error":"bad request"}""",
                        ContentType.Application.Json,
                        HttpStatusCode.BadRequest
                    )
                }
            }

            post("/flavour") {
                try {
                    val request = json.decodeFromString(OrderFlavourRequest.serializer(), call.receiveText())
                    val shouldFail = Random.nextInt(100) < config.errorRatePercent
                    delay(delaySampler.sampleDelayMillis(latencySettings.get().flavour))

                    if (shouldFail) {
                        call.respondText(
                            """{"error":"model temporarily unavailable","trace_id":"${UUID.randomUUID()}"}""",
                            ContentType.Application.Json,
                            HttpStatusCode.ServiceUnavailable
                        )
                        return@post
                    }

                    val response = analyseFlavour(request)
                    call.respondText(
                        json.encodeToString(OrderFlavourResponse.serializer(), response),
                        ContentType.Application.Json
                    )
                } catch (_: Exception) {
                    call.respondText(
                        """{"error":"bad request"}""",
                        ContentType.Application.Json,
                        HttpStatusCode.BadRequest
                    )
                }
            }
        }
    }

    println(
        "demo-stubs listening on port=${config.port} workers=${config.workers} " +
                "etaDelays=${config.etaLatency.delayP90Ms}/${config.etaLatency.delayP95Ms}/${config.etaLatency.delayP99Ms}/${config.etaLatency.delayP100Ms}ms " +
                "flavourDelays=${config.flavourLatency.delayP90Ms}/${config.flavourLatency.delayP95Ms}/${config.flavourLatency.delayP99Ms}/${config.flavourLatency.delayP100Ms}ms " +
                "errorRate=${config.errorRatePercent}%"
    )

    server.start(wait = true)
}

private fun analyseFlavour(request: OrderFlavourRequest): OrderFlavourResponse {
    val palette = when (abs(request.customerId.hashCode()) % 4) {
        0 -> "ember-gold"
        1 -> "moonlit-teal"
        2 -> "violet-smoke"
        else -> "moss-and-silver"
    }
    val moonPhase = if (request.orderedAt.takeLast(2).firstOrNull()?.isDigit() == true) "full_moon" else "waxing_gibbous"
    val correction = 0.92 + (abs(request.orderId.hashCode()) % 18) / 100.0

    return OrderFlavourResponse(
        requestId = UUID.randomUUID().toString(),
        flavourProfileId = "flavour-${abs(request.customerId.hashCode()).toString(16)}",
        palette = palette,
        etaCorrectionFactor = correction,
        moonPhase = moonPhase
    )
}

private fun estimate(request: ArcaneEtaRequest): ArcaneEtaResponse {
    val magicalEtaUnits =
        420.0 +
                abs(request.temperatureC - 93.0) * 11.0 +
                abs(request.densitySg - 1.18) * 160.0

    val moonPhase = if (request.temperatureC >= 90.0) "waxing_gibbous" else "waning_crescent"
    val planetaryAlignment = if (request.densitySg >= 1.21) "volatile" else "favorable"

    return ArcaneEtaResponse(
        requestId = UUID.randomUUID().toString(),
        regulatoryTraceId = "mrb-${request.batchId}-${request.cauldronId}",
        magicalEtaUnits = magicalEtaUnits,
        moonPhase = moonPhase,
        planetaryAlignment = planetaryAlignment
    )
}

@Serializable
data class ArcaneEtaRequest(
    @SerialName("batch_id") val batchId: String,
    @SerialName("recipe_id") val recipeId: String?,
    @SerialName("cauldron_id") val cauldronId: String,
    @SerialName("temperature_c") val temperatureC: Double,
    @SerialName("density_sg") val densitySg: Double,
    @SerialName("previous_temperature_c") val previousTemperatureC: Double? = null,
    @SerialName("previous_density_sg") val previousDensitySg: Double? = null,
    @SerialName("previous_bubble_rate_hz") val previousBubbleRateHz: Double? = null,
    @SerialName("previous_magical_eta_units") val previousMagicalEtaUnits: Double? = null
)

@Serializable
data class ArcaneEtaResponse(
    @SerialName("request_id") val requestId: String,
    @SerialName("regulatory_trace_id") val regulatoryTraceId: String,
    @SerialName("magical_eta_units") val magicalEtaUnits: Double,
    @SerialName("moon_phase") val moonPhase: String,
    @SerialName("planetary_alignment") val planetaryAlignment: String
)

@Serializable
data class ModelLatencyRegistry(
    val eta: ModelLatencySettings,
    val flavour: ModelLatencySettings
)

@Serializable
data class OrderFlavourRequest(
    @SerialName("order_id") val orderId: String,
    @SerialName("customer_id") val customerId: String,
    @SerialName("recipe_id") val recipeId: String?,
    @SerialName("potion_id") val potionId: String,
    @SerialName("ordered_at") val orderedAt: String
)

@Serializable
data class OrderFlavourResponse(
    @SerialName("request_id") val requestId: String,
    @SerialName("flavour_profile_id") val flavourProfileId: String,
    @SerialName("palette") val palette: String,
    @SerialName("eta_correction_factor") val etaCorrectionFactor: Double,
    @SerialName("moon_phase") val moonPhase: String
)
