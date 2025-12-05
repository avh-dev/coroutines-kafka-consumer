plugins {
    kotlin("jvm") version "2.0.0"
}

description = "Core coroutine-based Kafka consumer library"

kotlin {
    jvmToolchain(21)
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}