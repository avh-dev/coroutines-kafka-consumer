package avh.ckc.spring

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean

/** Shared bounded process-termination infrastructure, independent of managed consumer startup. */
@AutoConfiguration(before = [CkcSpringBootAutoConfiguration::class])
class CkcTerminalFailureAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun ckcTerminalFailureExitHandler(): CkcTerminalFailureExitHandler =
        CkcTerminalFailureExitHandler { status -> System.exit(status) }

    @Bean
    @ConditionalOnMissingBean
    fun ckcTerminalFailureHaltHandler(): CkcTerminalFailureHaltHandler =
        CkcTerminalFailureHaltHandler { status -> Runtime.getRuntime().halt(status) }

    @Bean
    @ConditionalOnMissingBean
    fun ckcTerminalFailureCoordinator(
        applicationContext: ApplicationContext,
        exitHandler: CkcTerminalFailureExitHandler,
        haltHandler: CkcTerminalFailureHaltHandler
    ): CkcTerminalFailureCoordinator =
        CkcTerminalFailureCoordinator(applicationContext, exitHandler, haltHandler)
}
