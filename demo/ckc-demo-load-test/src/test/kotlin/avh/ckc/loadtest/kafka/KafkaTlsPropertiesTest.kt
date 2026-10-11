package avh.ckc.loadtest.kafka

import org.apache.kafka.common.config.SslConfigs
import kotlin.test.Test
import kotlin.test.assertEquals

class KafkaTlsPropertiesTest {
    @Test
    fun `PEM truststore certificates configure a PEM truststore`() {
        val certificates = "-----BEGIN CERTIFICATE-----\\nca\\n-----END CERTIFICATE-----"

        assertEquals(
            mapOf(
                SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG to certificates,
                SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG to "PEM"
            ),
            kafkaPemTruststoreProperties(certificates)
        )
    }
}
