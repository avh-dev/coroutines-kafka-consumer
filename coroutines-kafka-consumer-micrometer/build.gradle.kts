import org.gradle.api.JavaVersion
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm")
}

description = "Micrometer adapter for coroutine-based Kafka consumer telemetry"

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

val micrometerVersion = "1.15.0"
val junitVersion = "5.10.2"

dependencies {
    api(project(":coroutines-kafka-consumer-core"))
    implementation("io.micrometer:micrometer-core")

    testImplementation(kotlin("test-junit5"))
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testImplementation("io.micrometer:micrometer-core")
    testImplementation("io.micrometer:micrometer-registry-prometheus")

    constraints {
        implementation("io.micrometer:micrometer-core") {
            version { require(micrometerVersion) }
            because("Micrometer adapter depends on micrometer-core")
        }

        testImplementation("io.micrometer:micrometer-core") {
            version { require(micrometerVersion) }
            because("Tests verify metrics against a real registry")
        }

        testImplementation("io.micrometer:micrometer-registry-prometheus") {
            version { require(micrometerVersion) }
            because("Tests verify Prometheus-specific label compatibility")
        }

        testImplementation("org.junit.jupiter:junit-jupiter-api") {
            version { require(junitVersion) }
            because("Tests run on JUnit 5")
        }

        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine") {
            version { require(junitVersion) }
            because("JUnit 5 engine is required to execute tests")
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
