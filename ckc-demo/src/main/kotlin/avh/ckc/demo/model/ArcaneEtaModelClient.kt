package avh.ckc.demo.model

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.CompletionStage

class ArcaneEtaModelClient(
    private val baseUri: URI,
    private val httpClient: HttpClient = HttpClient.newHttpClient(),
    private val json: Json = Json { ignoreUnknownKeys = true }
) {
    fun estimate(request: ArcaneEtaRequest): CompletionStage<ArcaneEtaResponse> {
        val httpRequest = HttpRequest.newBuilder(baseUri.resolve("/eta"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(json.encodeToString(ArcaneEtaRequest.serializer(), request)))
            .build()

        return httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                check(response.statusCode() == 200) {
                    "Arcane ETA model responded with ${response.statusCode()}: ${response.body()}"
                }

                json.decodeFromString(ArcaneEtaResponse.serializer(), response.body())
            }
    }
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
