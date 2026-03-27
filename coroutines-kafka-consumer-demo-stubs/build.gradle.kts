import org.gradle.api.JavaVersion
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
    application
}

description = "Lightweight HTTP stub service for the demo Kafka consumer application"

kotlin {
    jvmToolchain(21)

    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

val serializationVersion = "1.7.1"
val junitVersion = "5.10.2"
val ktorVersion = "2.3.12"

dependencies {
    implementation(kotlin("stdlib"))
    implementation("io.ktor:ktor-server-core-jvm")
    implementation("io.ktor:ktor-server-netty-jvm")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json")

    testImplementation(kotlin("test-junit5"))
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    constraints {
        implementation("io.ktor:ktor-server-core-jvm") {
            version { require(ktorVersion) }
        }
        implementation("io.ktor:ktor-server-netty-jvm") {
            version { require(ktorVersion) }
        }
        implementation("org.jetbrains.kotlinx:kotlinx-serialization-json") {
            version { require(serializationVersion) }
        }
        testImplementation("org.junit.jupiter:junit-jupiter-api") {
            version { require(junitVersion) }
        }
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine") {
            version { require(junitVersion) }
        }
    }
}

application {
    mainClass.set("avh.ckc.demostubs.DemoStubsApplicationKt")
}

tasks.test {
    useJUnitPlatform()
}
