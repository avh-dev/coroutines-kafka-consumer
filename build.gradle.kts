import org.gradle.api.tasks.bundling.AbstractArchiveTask
import org.gradle.api.tasks.Sync

val publishedArtifactNames = mapOf(
    "ckc-core" to "coroutines-kafka-consumer-core",
    "ckc-micrometer" to "coroutines-kafka-consumer-micrometer",
    "ckc-spring-boot-starter" to "coroutines-kafka-consumer-spring-boot-starter",
    "ckc-experiments" to "coroutines-kafka-consumer-experiments",
    "ckc-demo-contracts" to "coroutines-kafka-consumer-demo-contracts",
    "ckc-demo" to "coroutines-kafka-consumer-demo",
    "ckc-demo-load-test" to "coroutines-kafka-consumer-demo-load-test",
    "ckc-demo-stubs" to "coroutines-kafka-consumer-demo-stubs",
)

allprojects {
    group = "dev.avh"
    version = "0.0.1"

    repositories {
        mavenLocal()
        mavenCentral()
    }
}

plugins {
    kotlin("jvm") version "2.0.0" apply false
    kotlin("plugin.serialization") version "2.0.0" apply false
    id("com.google.protobuf") version "0.9.4" apply false
    id("org.springframework.boot") version "3.3.5" apply false
}

val threadStatsVersion = providers.gradleProperty("threadStatsVersion").get()
val threadStatsAgent by configurations.creating {
    isCanBeConsumed = false
    isCanBeResolved = true
}

dependencies {
    add(threadStatsAgent.name, "dev.avh.threadstats:thread-stats-agent:$threadStatsVersion") {
        isTransitive = false
    }
}

tasks.register<Sync>("stageThreadStatsAgent") {
    from(threadStatsAgent)
    into(layout.buildDirectory.dir("internal-lab/thread-stats-agent/$threadStatsVersion"))
    rename { "thread-stats-agent.jar" }
}

subprojects {
    publishedArtifactNames[name]?.let { artifactName ->
        tasks.withType<AbstractArchiveTask>().configureEach {
            archiveBaseName.set(artifactName)
        }
    }
}
