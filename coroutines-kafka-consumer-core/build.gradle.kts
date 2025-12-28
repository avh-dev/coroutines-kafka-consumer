plugins {
    kotlin("jvm") version "2.0.0"
    `java-test-fixtures`
    id("me.champeau.jmh") version "0.7.3"
}

description = "Core coroutine-based Kafka consumer library"

kotlin {
    jvmToolchain(21)
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")

    // --- tests / testFixtures ---
    testImplementation(kotlin("test"))

    testFixturesImplementation(kotlin("test"))

    // --- JMH ---
    val jmhVersion = "1.37"
    jmhImplementation("org.openjdk.jmh:jmh-core:$jmhVersion")
    jmhImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")
    jmhAnnotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:$jmhVersion")
    jmhImplementation(sourceSets.named("testFixtures").get().output)
}

jmh {
//    includes.set(listOf(".*OffsetTrackerConcurrencyBenchmark.*"))
    includes.set(listOf(".*OffsetTrackerBenchmark.*"))
}

tasks.test {
    useJUnitPlatform()

    testLogging {
        events("PASSED", "FAILED", "SKIPPED")
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }
}