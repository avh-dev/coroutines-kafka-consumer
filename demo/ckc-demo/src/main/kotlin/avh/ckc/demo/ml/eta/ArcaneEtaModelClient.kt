package avh.ckc.demo.ml.eta

import io.ktor.client.HttpClient as KtorHttpClient
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.withContext
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
    private val json: Json = Json { ignoreUnknownKeys = true }
) : SyncArcaneEtaModelClient {
    override fun estimate(request: ArcaneEtaRequest): ArcaneEtaResponse {
        val response = httpClient.send(httpRequest(request), HttpResponse.BodyHandlers.ofString())
        return decodeResponse(response)
    }

    private fun httpRequest(request: ArcaneEtaRequest): HttpRequest =
        newArcaneEtaHttpRequest(baseUri, json, request)

    private fun decodeResponse(response: HttpResponse<String>): ArcaneEtaResponse =
        decodeArcaneEtaResponse(json, response)
}

class KtorSuspendArcaneEtaModelClient(
    private val baseUri: URI,
    private val httpClient: KtorHttpClient,
    private val dispatcher: CoroutineDispatcher,
    private val json: Json = Json { ignoreUnknownKeys = true }
) : SuspendArcaneEtaModelClient {
    override suspend fun estimate(request: ArcaneEtaRequest): ArcaneEtaResponse = withContext(dispatcher) {
        val response = httpClient.post(baseUri.resolve("/eta").toString()) {
            contentType(ContentType.Application.Json)
            setBody(json.encodeToString(ArcaneEtaRequest.serializer(), request))
        }
        decodeResponse(response)
    }

    private suspend fun decodeResponse(response: io.ktor.client.statement.HttpResponse): ArcaneEtaResponse =
        decodeArcaneEtaResponse(json, response.status.value, response.bodyAsText())
}

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
