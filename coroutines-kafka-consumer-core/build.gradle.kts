import org.gradle.api.JavaVersion
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm")
    `java-test-fixtures`
}

description = "Core coroutine-based Kafka consumer library"

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

val coroutinesVersion = "1.9.0"
val kafkaVersion = "3.4.0"
val slf4jVersion = "2.0.12"
val junitVersion = "5.10.2"
val mockitoVersion = "5.12.0"
val mockitoKotlinVersion = "5.3.1"

dependencies {
    implementation(kotlin("stdlib"))

    api("org.apache.kafka:kafka-clients")
    api("org.slf4j:slf4j-api")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")

    testFixturesImplementation(kotlin("test-junit5"))
    testFixturesImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")

    testImplementation(kotlin("test-junit5"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test")
    testImplementation(testFixtures(project(":coroutines-kafka-consumer-core")))

    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    testImplementation("org.mockito:mockito-core")
    testImplementation("org.mockito:mockito-junit-jupiter")
    testImplementation("org.mockito.kotlin:mockito-kotlin")

    constraints {
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core") {
            version { require(coroutinesVersion) }
            because("Library requires coroutines $coroutinesVersion+")
        }

        testFixturesImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-core") {
            version { require(coroutinesVersion) }
            because("Test fixtures use coroutines")
        }

        testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test") {
            version { require(coroutinesVersion) }
            because("Tests rely on coroutine test utilities compatible with coroutines core")
        }

        api("org.apache.kafka:kafka-clients") {
            version { require(kafkaVersion) }
            because("Uses APIs introduced in Kafka $kafkaVersion+")
        }

        api("org.slf4j:slf4j-api") {
            version { require(slf4jVersion) }
            because("Library logs via SLF4J API")
        }

        testImplementation("org.junit.jupiter:junit-jupiter-api") {
            version { require(junitVersion) }
            because("Tests run on JUnit 5")
        }

        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine") {
            version { require(junitVersion) }
            because("JUnit 5 engine is required to execute tests")
        }

        testImplementation("org.mockito:mockito-core") {
            version { require(mockitoVersion) }
            because("Tests mock Kafka client interactions")
        }

        testImplementation("org.mockito:mockito-junit-jupiter") {
            version { require(mockitoVersion) }
            because("Mockito JUnit 5 integration is used in tests")
        }

        testImplementation("org.mockito.kotlin:mockito-kotlin") {
            version { require(mockitoKotlinVersion) }
            because("Tests use Kotlin-friendly Mockito DSL")
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