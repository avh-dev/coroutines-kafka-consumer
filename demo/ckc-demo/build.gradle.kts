import org.gradle.api.JavaVersion
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
    id("org.springframework.boot")
    application
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
val armeriaVersion = "1.39.0"
val parallelConsumerVersion = "0.5.3.3"
val serializationVersion = "1.7.1"
val springBootVersion = "3.5.10"

dependencies {
    implementation(project(":ckc-core"))
    implementation(project(":ckc-demo-contracts"))
    implementation(project(":ckc-micrometer"))
    implementation(project(":ckc-spring-boot-starter"))
    implementation(platform("org.springframework.boot:spring-boot-dependencies:$springBootVersion"))
    implementation(kotlin("reflect"))
    implementation(kotlin("stdlib"))
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json")
    implementation("com.linecorp.armeria:armeria-spring-boot3-starter:$armeriaVersion")
    implementation("com.linecorp.armeria:armeria-spring-boot3-actuator-starter:$armeriaVersion")
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("io.lettuce:lettuce-core")
    implementation("io.micrometer:micrometer-registry-prometheus")
    implementation("io.confluent.parallelconsumer:parallel-consumer-core:$parallelConsumerVersion")
    implementation("io.confluent.parallelconsumer:parallel-consumer-reactor:$parallelConsumerVersion")
    implementation("net.logstash.logback:logstash-logback-encoder:8.1")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactive")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")

    testImplementation(kotlin("test-junit5"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    constraints {
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core") {
            version { require(coroutinesVersion) }
        }
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8") {
            version { require(coroutinesVersion) }
        }
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactive") {
            version { require(coroutinesVersion) }
        }
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor") {
            version { require(coroutinesVersion) }
        }
        implementation("org.jetbrains.kotlinx:kotlinx-serialization-json") {
            version { require(serializationVersion) }
        }
    }
}

application {
    mainClass.set("avh.ckc.demo.DemoApplicationKt")
}

tasks.jar {
    enabled = true
}

tasks.bootJar {
    enabled = false
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
