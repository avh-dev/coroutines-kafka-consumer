import org.gradle.api.JavaVersion
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
    id("org.springframework.boot")
}

description = "Demo application for coroutine-based Kafka consumer with protobuf payloads"

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
val springBootVersion = "3.3.5"

dependencies {
    implementation(project(":ckc-core"))
    implementation(project(":ckc-demo-contracts"))
    implementation(project(":ckc-micrometer"))
    implementation(platform("org.springframework.boot:spring-boot-dependencies:$springBootVersion"))
    implementation(kotlin("reflect"))
    implementation(kotlin("stdlib"))
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json")
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("io.micrometer:micrometer-registry-prometheus")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.springframework:spring-web")

    testImplementation(kotlin("test-junit5"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")

    constraints {
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core") {
            version { require(coroutinesVersion) }
        }
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8") {
            version { require(coroutinesVersion) }
        }
        implementation("org.jetbrains.kotlinx:kotlinx-serialization-json") {
            version { require(serializationVersion) }
        }
    }
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
