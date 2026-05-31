package avh.ckc.demo.ml.flavour

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

interface SyncOrderFlavourModelClient {
    fun analyse(request: OrderFlavourRequest): OrderFlavourResponse
}

interface SuspendOrderFlavourModelClient {
    suspend fun analyse(request: OrderFlavourRequest): OrderFlavourResponse
}

class JdkSyncOrderFlavourModelClient(
    private val baseUri: URI,
    private val httpClient: JdkHttpClient = JdkHttpClient.newHttpClient(),
    private val json: Json = Json { ignoreUnknownKeys = true },
    private val modelCallMetrics: ModelCallMetrics? = null
) : SyncOrderFlavourModelClient {
    override fun analyse(request: OrderFlavourRequest): OrderFlavourResponse = recordCall("sync", "jdk") {
        val response = httpClient.send(httpRequest(request), HttpResponse.BodyHandlers.ofString())
        decodeResponse(response)
    }

    private fun httpRequest(request: OrderFlavourRequest): HttpRequest =
        newOrderFlavourHttpRequest(baseUri, json, request)

    private fun decodeResponse(response: HttpResponse<String>): OrderFlavourResponse =
        decodeOrderFlavourResponse(json, response)

    private fun recordCall(
        clientMode: String,
        transport: String,
        block: () -> OrderFlavourResponse
    ): OrderFlavourResponse =
        modelCallMetrics?.record(MODEL_NAME, OPERATION_NAME, clientMode, transport, block) ?: block()
}

class ArmeriaSuspendOrderFlavourModelClient(
    private val webClient: WebClient,
    private val json: Json = Json { ignoreUnknownKeys = true },
    private val modelCallMetrics: ModelCallMetrics? = null
) : SuspendOrderFlavourModelClient {
    override suspend fun analyse(request: OrderFlavourRequest): OrderFlavourResponse = recordCall("suspend", "armeria") {
        val response = webClient.execute(
            RequestHeaders.of(
                HttpMethod.POST,
                "/flavour",
                HttpHeaderNames.CONTENT_TYPE,
                MediaType.JSON_UTF_8
            ),
            HttpData.ofUtf8(json.encodeToString(OrderFlavourRequest.serializer(), request))
        ).aggregate().await()
        decodeOrderFlavourResponse(json, response.status().code(), response.contentUtf8())
    }

    private suspend fun recordCall(
        clientMode: String,
        transport: String,
        block: suspend () -> OrderFlavourResponse
    ): OrderFlavourResponse =
        modelCallMetrics?.recordSuspend(MODEL_NAME, OPERATION_NAME, clientMode, transport, block) ?: block()
}

private const val MODEL_NAME = "order_flavour"
private const val OPERATION_NAME = "analyse"

private fun newOrderFlavourHttpRequest(baseUri: URI, json: Json, request: OrderFlavourRequest): HttpRequest =
    HttpRequest.newBuilder(baseUri.resolve("/flavour"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(json.encodeToString(OrderFlavourRequest.serializer(), request)))
        .build()

private fun decodeOrderFlavourResponse(json: Json, response: HttpResponse<String>): OrderFlavourResponse {
    return decodeOrderFlavourResponse(json, response.statusCode(), response.body())
}

private fun decodeOrderFlavourResponse(json: Json, statusCode: Int, body: String): OrderFlavourResponse {
    check(statusCode == 200) {
        "Order flavour model responded with $statusCode: $body"
    }

    return json.decodeFromString(OrderFlavourResponse.serializer(), body)
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
