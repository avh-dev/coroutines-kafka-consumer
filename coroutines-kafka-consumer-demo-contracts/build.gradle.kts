import org.gradle.api.JavaVersion
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm")
    id("com.google.protobuf")
}

description = "Shared protobuf contracts and Kafka serde for demo modules"

kotlin {
    jvmToolchain(21)

    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_11)
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

val kafkaVersion = "3.4.0"
val protobufVersion = "3.25.3"
val junitVersion = "5.10.2"

dependencies {
    implementation(kotlin("stdlib"))
    api("org.apache.kafka:kafka-clients")
    api("com.google.protobuf:protobuf-java")

    testImplementation(kotlin("test-junit5"))
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    constraints {
        api("org.apache.kafka:kafka-clients") {
            version { require(kafkaVersion) }
        }
        api("com.google.protobuf:protobuf-java") {
            version { require(protobufVersion) }
        }
        testImplementation("org.junit.jupiter:junit-jupiter-api") {
            version { require(junitVersion) }
        }
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine") {
            version { require(junitVersion) }
        }
    }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
}

tasks.test {
    useJUnitPlatform()
}
