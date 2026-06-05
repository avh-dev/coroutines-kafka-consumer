package avh.ckc.demo.registry

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

interface SyncBrewingStepRegistryClient {
    fun reportStep(request: BrewingStepRegistryRequest): BrewingStepRegistryResponse
}

interface SuspendBrewingStepRegistryClient {
    suspend fun reportStep(request: BrewingStepRegistryRequest): BrewingStepRegistryResponse
}

class JdkSyncBrewingStepRegistryClient(
    private val baseUri: URI,
    private val httpClient: JdkHttpClient = JdkHttpClient.newHttpClient(),
    private val json: Json = Json { ignoreUnknownKeys = true },
    private val modelCallMetrics: ModelCallMetrics? = null
) : SyncBrewingStepRegistryClient {
    override fun reportStep(request: BrewingStepRegistryRequest): BrewingStepRegistryResponse = recordCall("sync", "jdk") {
        val response = httpClient.send(httpRequest(request), HttpResponse.BodyHandlers.ofString())
        decodeBrewingStepRegistryResponse(json, response.statusCode(), response.body())
    }

    private fun httpRequest(request: BrewingStepRegistryRequest): HttpRequest =
        HttpRequest.newBuilder(baseUri.resolve(REGISTRY_PATH))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(json.encodeToString(BrewingStepRegistryRequest.serializer(), request)))
            .build()

    private fun recordCall(
        clientMode: String,
        transport: String,
        block: () -> BrewingStepRegistryResponse
    ): BrewingStepRegistryResponse =
        modelCallMetrics?.record(MODEL_NAME, OPERATION_NAME, clientMode, transport, block) ?: block()
}

class ArmeriaSuspendBrewingStepRegistryClient(
    private val webClient: WebClient,
    private val json: Json = Json { ignoreUnknownKeys = true },
    private val modelCallMetrics: ModelCallMetrics? = null
) : SuspendBrewingStepRegistryClient {
    override suspend fun reportStep(request: BrewingStepRegistryRequest): BrewingStepRegistryResponse =
        recordCall("suspend", "armeria") {
            val response = webClient.execute(
                RequestHeaders.of(
                    HttpMethod.POST,
                    REGISTRY_PATH,
                    HttpHeaderNames.CONTENT_TYPE,
                    MediaType.JSON_UTF_8
                ),
                HttpData.ofUtf8(json.encodeToString(BrewingStepRegistryRequest.serializer(), request))
            ).aggregate().await()
            decodeBrewingStepRegistryResponse(json, response.status().code(), response.contentUtf8())
        }

    private suspend fun recordCall(
        clientMode: String,
        transport: String,
        block: suspend () -> BrewingStepRegistryResponse
    ): BrewingStepRegistryResponse =
        modelCallMetrics?.recordSuspend(MODEL_NAME, OPERATION_NAME, clientMode, transport, block) ?: block()
}

private fun decodeBrewingStepRegistryResponse(
    json: Json,
    statusCode: Int,
    body: String
): BrewingStepRegistryResponse {
    check(statusCode == 200) {
        "Brewing step registry responded with $statusCode: $body"
    }
    return json.decodeFromString(BrewingStepRegistryResponse.serializer(), body)
}

private const val REGISTRY_PATH = "/brewing-registry/steps"
private const val MODEL_NAME = "brewing_registry"
private const val OPERATION_NAME = "report_step"

@Serializable
data class BrewingStepRegistryRequest(
    @SerialName("batch_id") val batchId: String,
    @SerialName("cauldron_id") val cauldronId: String,
    @SerialName("step_number") val stepNumber: Int,
    @SerialName("step_code") val stepCode: String,
    @SerialName("completed_at") val completedAt: String,
    @SerialName("regulatory_trace_id") val regulatoryTraceId: String
)

@Serializable
data class BrewingStepRegistryResponse(
    @SerialName("receipt_id") val receiptId: String,
    @SerialName("accepted_at") val acceptedAt: String,
    @SerialName("registry_shard") val registryShard: String
)
