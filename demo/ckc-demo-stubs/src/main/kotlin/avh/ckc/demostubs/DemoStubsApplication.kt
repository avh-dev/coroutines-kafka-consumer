package avh.ckc.demostubs

import com.linecorp.armeria.common.AggregatedHttpRequest
import com.linecorp.armeria.common.HttpData
import com.linecorp.armeria.common.HttpHeaderNames
import com.linecorp.armeria.common.HttpMethod
import com.linecorp.armeria.common.HttpRequest
import com.linecorp.armeria.common.HttpResponse
import com.linecorp.armeria.common.HttpStatus
import com.linecorp.armeria.common.MediaType
import com.linecorp.armeria.common.ResponseHeaders
import com.linecorp.armeria.common.util.EventLoopGroups
import com.linecorp.armeria.server.Server
import com.linecorp.armeria.server.ServiceRequestContext
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.math.abs
import kotlin.random.Random

fun main() {
    val config = DemoStubsConfig.fromEnvironment()
    val json = Json { ignoreUnknownKeys = true }
    val delaySampler = DelaySampler(Random.Default)
    val settings = DemoStubsSettingsState(
        RedisDemoStubsSettingsStore("redis://${config.redisHost}:${config.redisPort}", json)
    )
    val workerGroup = EventLoopGroups.newEventLoopGroup(config.workers, "demo-stubs-armeria-worker")
    val server = Server.builder()
        .http(config.port)
        .workerGroup(workerGroup, true)
        .service("/health") { _, _ ->
            jsonResponse("""{"status":"UP"}""")
        }
        .service("/settings") { _, request ->
            when (request.method()) {
                HttpMethod.GET -> jsonResponse(json.encodeToString(DemoStubsSettings.serializer(), settings.get()))
                HttpMethod.POST -> aggregate(request) { aggregated ->
                    val update = try {
                        json.decodeFromString(DemoStubsSettings.serializer(), aggregated.contentUtf8())
                    } catch (_: Exception) {
                        return@aggregate jsonResponse("""{"error":"bad request"}""", HttpStatus.BAD_REQUEST)
                    }
                    try {
                        settings.update(update)
                        jsonResponse(json.encodeToString(DemoStubsSettings.serializer(), update))
                    } catch (error: Exception) {
                        System.err.println("Failed to persist demo-stubs settings in Redis: ${error.message}")
                        jsonResponse("""{"error":"settings persistence failed"}""", HttpStatus.SERVICE_UNAVAILABLE)
                    }
                }

                else -> methodNotAllowed()
            }
        }
        .service("/eta") { ctx, request ->
            if (request.method() != HttpMethod.POST) {
                methodNotAllowed()
            } else {
                aggregate(request) { aggregated ->
                    try {
                        val modelRequest = json.decodeFromString(ArcaneEtaRequest.serializer(), aggregated.contentUtf8())
                        val currentSettings = settings.get()
                        scheduledResponse(ctx, delaySampler.sampleDelayMillis(currentSettings.eta)) {
                            if (Random.nextInt(100) < currentSettings.errorRatePercent) {
                                jsonResponse(
                                    """{"error":"model temporarily unavailable","trace_id":"${UUID.randomUUID()}"}""",
                                    HttpStatus.SERVICE_UNAVAILABLE
                                )
                            } else {
                                jsonResponse(json.encodeToString(ArcaneEtaResponse.serializer(), estimate(modelRequest)))
                            }
                        }
                    } catch (_: Exception) {
                        jsonResponse("""{"error":"bad request"}""", HttpStatus.BAD_REQUEST)
                    }
                }
            }
        }
        .service("/flavour") { ctx, request ->
            if (request.method() != HttpMethod.POST) {
                methodNotAllowed()
            } else {
                aggregate(request) { aggregated ->
                    try {
                        val modelRequest = json.decodeFromString(OrderFlavourRequest.serializer(), aggregated.contentUtf8())
                        val currentSettings = settings.get()
                        scheduledResponse(ctx, delaySampler.sampleDelayMillis(currentSettings.flavour)) {
                            if (Random.nextInt(100) < currentSettings.errorRatePercent) {
                                jsonResponse(
                                    """{"error":"model temporarily unavailable","trace_id":"${UUID.randomUUID()}"}""",
                                    HttpStatus.SERVICE_UNAVAILABLE
                                )
                            } else {
                                jsonResponse(json.encodeToString(OrderFlavourResponse.serializer(), analyseFlavour(modelRequest)))
                            }
                        }
                    } catch (_: Exception) {
                        jsonResponse("""{"error":"bad request"}""", HttpStatus.BAD_REQUEST)
                    }
                }
            }
        }
        .build()

    println(
        "demo-stubs listening on port=${config.port} workers=${config.workers} settings=${settings.get()}"
    )

    Runtime.getRuntime().addShutdownHook(Thread {
        try {
            server.stop().join()
        } finally {
            settings.close()
        }
    })
    server.start().join()
    Thread.currentThread().join()
}

private fun aggregate(
    request: HttpRequest,
    handle: (AggregatedHttpRequest) -> HttpResponse
): HttpResponse =
    HttpResponse.of(request.aggregate().thenApply(handle))

private fun scheduledResponse(
    context: ServiceRequestContext,
    delayMillis: Long,
    response: () -> HttpResponse
): HttpResponse {
    if (delayMillis <= 0) {
        return response()
    }
    val future = CompletableFuture<HttpResponse>()
    context.eventLoop().schedule(
        { future.complete(response()) },
        delayMillis,
        TimeUnit.MILLISECONDS
    )
    return HttpResponse.of(future)
}

private fun jsonResponse(body: String, status: HttpStatus = HttpStatus.OK): HttpResponse =
    HttpResponse.of(
        ResponseHeaders.of(
            status,
            HttpHeaderNames.CONTENT_TYPE,
            MediaType.JSON_UTF_8
        ),
        HttpData.ofUtf8(body)
    )

private fun methodNotAllowed(): HttpResponse =
    jsonResponse("""{"error":"method not allowed"}""", HttpStatus.METHOD_NOT_ALLOWED)

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
