package avh.ckc.demo

import avh.ckc.spring.CkcTerminalFailureCoordinator
import avh.ckc.spring.CkcTerminalFailureExitHandler
import avh.ckc.spring.CkcTerminalFailureHaltHandler
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean

@TestConfiguration(proxyBeanMethods = false)
class ManualTerminalFailureTestConfiguration {
    @Bean
    fun manualTerminalFailureExitHandler(): CkcTerminalFailureExitHandler = exitHandler

    @Bean
    fun manualTerminalFailureHaltHandler(): CkcTerminalFailureHaltHandler = haltHandler

    @Bean
    fun manualTerminalFailureDependency(
        coordinator: CkcTerminalFailureCoordinator
    ): ManualTerminalFailureDependency = ManualTerminalFailureDependency(coordinator)

    companion object {
        val exitHandler = CkcTerminalFailureExitHandler { }
        val haltHandler = CkcTerminalFailureHaltHandler { }
    }
}

class ManualTerminalFailureDependency(
    val coordinator: CkcTerminalFailureCoordinator
)
