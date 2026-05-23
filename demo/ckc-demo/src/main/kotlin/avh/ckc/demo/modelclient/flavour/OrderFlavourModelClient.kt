package avh.ckc.demo.modelclient.flavour

import kotlinx.coroutines.future.await
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

interface SyncOrderFlavourModelClient {
    fun analyse(request: OrderFlavourRequest): OrderFlavourResponse
}

interface SuspendOrderFlavourModelClient {
    suspend fun analyse(request: OrderFlavourRequest): OrderFlavourResponse
}

class JdkSyncOrderFlavourModelClient(
    private val baseUri: URI,
    private val httpClient: HttpClient = HttpClient.newHttpClient(),
    private val json: Json = Json { ignoreUnknownKeys = true }
) : SyncOrderFlavourModelClient {
    override fun analyse(request: OrderFlavourRequest): OrderFlavourResponse {
        val response = httpClient.send(httpRequest(request), HttpResponse.BodyHandlers.ofString())
        return decodeResponse(response)
    }

    private fun httpRequest(request: OrderFlavourRequest): HttpRequest =
        newOrderFlavourHttpRequest(baseUri, json, request)

    private fun decodeResponse(response: HttpResponse<String>): OrderFlavourResponse =
        decodeOrderFlavourResponse(json, response)
}

class JdkSuspendOrderFlavourModelClient(
    private val baseUri: URI,
    private val httpClient: HttpClient = HttpClient.newHttpClient(),
    private val json: Json = Json { ignoreUnknownKeys = true }
) : SuspendOrderFlavourModelClient {
    override suspend fun analyse(request: OrderFlavourRequest): OrderFlavourResponse {
        val response = httpClient.sendAsync(httpRequest(request), HttpResponse.BodyHandlers.ofString()).await()
        return decodeResponse(response)
    }

    private fun httpRequest(request: OrderFlavourRequest): HttpRequest =
        newOrderFlavourHttpRequest(baseUri, json, request)

    private fun decodeResponse(response: HttpResponse<String>): OrderFlavourResponse =
        decodeOrderFlavourResponse(json, response)
}

private fun newOrderFlavourHttpRequest(baseUri: URI, json: Json, request: OrderFlavourRequest): HttpRequest =
    HttpRequest.newBuilder(baseUri.resolve("/flavour"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(json.encodeToString(OrderFlavourRequest.serializer(), request)))
        .build()

private fun decodeOrderFlavourResponse(json: Json, response: HttpResponse<String>): OrderFlavourResponse {
    check(response.statusCode() == 200) {
        "Order flavour model responded with ${response.statusCode()}: ${response.body()}"
    }

    return json.decodeFromString(OrderFlavourResponse.serializer(), response.body())
}

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
