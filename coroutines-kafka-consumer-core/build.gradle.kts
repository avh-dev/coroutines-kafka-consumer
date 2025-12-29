plugins {
    kotlin("jvm")
    `java-test-fixtures`
}

description = "Core coroutine-based Kafka consumer library"

kotlin {
    jvmToolchain(21)
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")

    testFixturesImplementation(kotlin("test-junit5"))
    testImplementation(kotlin("test-junit5"))
    testImplementation(testFixtures(project(":coroutines-kafka-consumer-core")))
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