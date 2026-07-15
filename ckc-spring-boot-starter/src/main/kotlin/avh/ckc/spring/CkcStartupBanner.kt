package avh.ckc.spring

import java.util.logging.Logger

internal fun logStartupBanner() {
    logger.info(
        "\n" +
            "  ___ _  __ ___\n" +
            " / __| |/ // __|  v${ckcStarterVersion()}\n" +
            "| (__| ' <| (__   Coroutines Kafka Consumer\n" +
            " \\___|_|\\_\\\\___|\n"
    )
}

internal fun ckcStarterVersion(): String =
    CkcSpringBootAutoConfiguration::class.java.`package`?.implementationVersion
        ?: "dev"

internal val logger: Logger = Logger.getLogger(CkcSpringBootAutoConfiguration::class.java.name)
