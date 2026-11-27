package avh.ckc.demo.config

import org.junit.jupiter.api.Test
import kotlin.test.assertFailsWith

class ModelStubEndpointGroupsTest {
    @Test
    fun `accepts HTTP origins with and without trailing slash`() {
        ModelStubEndpointGroups(true).use { groups ->
            groups.client("http://localhost:8085")
            groups.client("http://localhost:8085/")
        }
    }

    @Test
    fun `rejects a non-root path`() {
        ModelStubEndpointGroups(true).use { groups ->
            assertFailsWith<IllegalArgumentException> {
                groups.client("http://localhost:8085/api")
            }
        }
    }
}
