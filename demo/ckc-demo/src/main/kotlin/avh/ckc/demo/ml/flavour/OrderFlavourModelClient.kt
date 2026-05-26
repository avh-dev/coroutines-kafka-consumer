package avh.ckc.demo.ml.flavour

import com.linecorp.armeria.client.WebClient
import com.linecorp.armeria.common.HttpData
import com.linecorp.armeria.common.HttpHeaderNames
import com.linecorp.armeria.common.HttpMethod
import com.linecorp.armeria.common.MediaType
import com.linecorp.armeria.common.RequestHeaders
import io.ktor.client.HttpClient as KtorHttpClient
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
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

class KtorSuspendOrderFlavourModelClient(
    private val baseUri: URI,
    private val httpClient: KtorHttpClient,
    private val dispatcher: CoroutineDispatcher,
    private val json: Json = Json { ignoreUnknownKeys = true }
) : SuspendOrderFlavourModelClient {
    override suspend fun analyse(request: OrderFlavourRequest): OrderFlavourResponse = withContext(dispatcher) {
        val response = httpClient.post(baseUri.resolve("/flavour").toString()) {
            contentType(ContentType.Application.Json)
            setBody(json.encodeToString(OrderFlavourRequest.serializer(), request))
        }
        decodeResponse(response)
    }

    private suspend fun decodeResponse(response: io.ktor.client.statement.HttpResponse): OrderFlavourResponse =
        decodeOrderFlavourResponse(json, response.status.value, response.bodyAsText())
}

class ArmeriaSuspendOrderFlavourModelClient(
    private val webClient: WebClient,
    private val json: Json = Json { ignoreUnknownKeys = true }
) : SuspendOrderFlavourModelClient {
    override suspend fun analyse(request: OrderFlavourRequest): OrderFlavourResponse {
        val response = webClient.execute(
            RequestHeaders.of(
                HttpMethod.POST,
                "/flavour",
                HttpHeaderNames.CONTENT_TYPE,
                MediaType.JSON_UTF_8
            ),
            HttpData.ofUtf8(json.encodeToString(OrderFlavourRequest.serializer(), request))
        ).aggregate().await()
        return decodeOrderFlavourResponse(json, response.status().code(), response.contentUtf8())
    }
}

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
