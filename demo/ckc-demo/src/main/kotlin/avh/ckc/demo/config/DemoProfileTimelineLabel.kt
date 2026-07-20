package avh.ckc.demo.config

import avh.ckc.demo.config.DemoApplicationProperties.ProcessingDispatcherType

class DemoProfileTimelineLabel(
    private val properties: DemoApplicationProperties
) {
    fun resolve(springProfile: String): String =
        when (springProfile) {
            "ckc" -> "ckc.${dispatcherName(defaultType = ProcessingDispatcherType.FIXED)}"
            "ckc-sync" -> "ckc-sync.${dispatcherName(defaultType = ProcessingDispatcherType.IO)}"
            "spring-kafka-coroutines-naive" ->
                "spring-kafka-coroutines-naive.${dispatcherName(defaultType = ProcessingDispatcherType.FIXED)}"
            "confluent-parallel" -> "cpc"
            "confluent-parallel-reactor" -> "cpc-reactor.${dispatcherName(defaultType = ProcessingDispatcherType.FIXED)}"
            else -> springProfile
        }

    private fun dispatcherName(defaultType: ProcessingDispatcherType): String =
        when (actualDispatcherType(defaultType)) {
            ProcessingDispatcherType.AUTO -> error("AUTO must be resolved before profile timeline label creation")
            ProcessingDispatcherType.DEFAULT -> "default"
            ProcessingDispatcherType.IO -> "io"
            ProcessingDispatcherType.FIXED -> "fixed.${properties.consumers.workerDispatcherThreads}"
            ProcessingDispatcherType.VIRTUAL -> "virtual-thread"
        }

    private fun actualDispatcherType(defaultType: ProcessingDispatcherType): ProcessingDispatcherType =
        when (properties.consumers.processingDispatcherType) {
            ProcessingDispatcherType.AUTO -> defaultType
            else -> properties.consumers.processingDispatcherType
        }
}
