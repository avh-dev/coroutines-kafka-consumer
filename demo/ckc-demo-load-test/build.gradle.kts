import org.gradle.api.JavaVersion
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
    application
}

description = "Load-test traffic generator for the demo Kafka consumer application"

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

val coroutinesVersion = "1.9.0"
val serializationVersion = "1.7.1"
val micrometerVersion = "1.15.0"
val junitVersion = "5.10.2"

dependencies {
    implementation(project(":ckc-demo-contracts"))
    implementation(kotlin("stdlib"))
    implementation("com.google.protobuf:protobuf-java")
    implementation("ch.qos.logback:logback-classic:1.5.25")
    implementation("net.logstash.logback:logstash-logback-encoder:8.1")
    implementation("io.micrometer:micrometer-core")
    implementation("io.micrometer:micrometer-registry-prometheus")
    implementation("org.apache.kafka:kafka-clients")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json")

    testImplementation(kotlin("test-junit5"))
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    constraints {
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core") {
            version { require(coroutinesVersion) }
        }
        implementation("org.jetbrains.kotlinx:kotlinx-serialization-json") {
            version { require(serializationVersion) }
        }
        implementation("io.micrometer:micrometer-core") {
            version { require(micrometerVersion) }
        }
        implementation("io.micrometer:micrometer-registry-prometheus") {
            version { require(micrometerVersion) }
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
    mainClass.set("avh.ckc.loadtest.LoadTestApplicationKt")
}

tasks.test {
    useJUnitPlatform()
}
