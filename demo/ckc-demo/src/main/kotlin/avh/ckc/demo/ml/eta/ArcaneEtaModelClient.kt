package avh.ckc.demo.ml.eta

import avh.ckc.demo.ml.ModelCallMetrics
import com.linecorp.armeria.client.WebClient
import com.linecorp.armeria.common.HttpData
import com.linecorp.armeria.common.HttpHeaderNames
import com.linecorp.armeria.common.HttpMethod
import com.linecorp.armeria.common.MediaType
import com.linecorp.armeria.common.RequestHeaders
import kotlinx.coroutines.future.await
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.net.URI
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpClient as JdkHttpClient

interface SyncArcaneEtaModelClient {
    fun estimate(request: ArcaneEtaRequest): ArcaneEtaResponse
}

interface SuspendArcaneEtaModelClient {
    suspend fun estimate(request: ArcaneEtaRequest): ArcaneEtaResponse
}

class JdkSyncArcaneEtaModelClient(
    private val baseUri: URI,
    private val httpClient: JdkHttpClient = JdkHttpClient.newHttpClient(),
    private val json: Json = Json { ignoreUnknownKeys = true },
    private val modelCallMetrics: ModelCallMetrics? = null
) : SyncArcaneEtaModelClient {
    override fun estimate(request: ArcaneEtaRequest): ArcaneEtaResponse = recordCall("sync", "jdk") {
        val response = httpClient.send(httpRequest(request), HttpResponse.BodyHandlers.ofString())
        decodeResponse(response)
    }

    private fun httpRequest(request: ArcaneEtaRequest): HttpRequest =
        newArcaneEtaHttpRequest(baseUri, json, request)

    private fun decodeResponse(response: HttpResponse<String>): ArcaneEtaResponse =
        decodeArcaneEtaResponse(json, response)

    private fun recordCall(clientMode: String, transport: String, block: () -> ArcaneEtaResponse): ArcaneEtaResponse =
        modelCallMetrics?.record(MODEL_NAME, OPERATION_NAME, clientMode, transport, block) ?: block()
}

class ArmeriaSuspendArcaneEtaModelClient(
    private val webClient: WebClient,
    private val json: Json = Json { ignoreUnknownKeys = true },
    private val modelCallMetrics: ModelCallMetrics? = null
) : SuspendArcaneEtaModelClient {
    override suspend fun estimate(request: ArcaneEtaRequest): ArcaneEtaResponse = recordCall("suspend", "armeria") {
        val response = webClient.execute(
            RequestHeaders.of(
                HttpMethod.POST,
                "/eta",
                HttpHeaderNames.CONTENT_TYPE,
                MediaType.JSON_UTF_8
            ),
            HttpData.ofUtf8(json.encodeToString(ArcaneEtaRequest.serializer(), request))
        ).aggregate().await()
        decodeArcaneEtaResponse(json, response.status().code(), response.contentUtf8())
    }

    private suspend fun recordCall(
        clientMode: String,
        transport: String,
        block: suspend () -> ArcaneEtaResponse
    ): ArcaneEtaResponse =
        modelCallMetrics?.recordSuspend(MODEL_NAME, OPERATION_NAME, clientMode, transport, block) ?: block()
}

private const val MODEL_NAME = "arcane_eta"
private const val OPERATION_NAME = "estimate"

private fun newArcaneEtaHttpRequest(baseUri: URI, json: Json, request: ArcaneEtaRequest): HttpRequest =
    HttpRequest.newBuilder(baseUri.resolve("/eta"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(json.encodeToString(ArcaneEtaRequest.serializer(), request)))
        .build()

private fun decodeArcaneEtaResponse(json: Json, response: HttpResponse<String>): ArcaneEtaResponse {
    return decodeArcaneEtaResponse(json, response.statusCode(), response.body())
}

private fun decodeArcaneEtaResponse(json: Json, statusCode: Int, body: String): ArcaneEtaResponse {
    check(statusCode == 200) {
        "Arcane ETA model responded with $statusCode: $body"
    }

    return json.decodeFromString(ArcaneEtaResponse.serializer(), body)
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
