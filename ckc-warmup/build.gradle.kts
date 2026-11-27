import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.Sync
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm")
}

description = "Optional traffic warm-up extension for coroutine-based Kafka consumers"

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

val brokerCliImplementation = configurations.dependencyScope("brokerCliImplementation")
val brokerCliRuntime = configurations.resolvable("brokerCliRuntime") {
    extendsFrom(brokerCliImplementation.get())
}

dependencies {
    api(project(":ckc-core"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")
    add(brokerCliImplementation.name, kotlin("stdlib"))
    add(brokerCliImplementation.name, "org.apache.kafka:kafka-clients:3.4.0")

    compileOnly("io.micrometer:micrometer-core:1.15.0")

    testImplementation(testFixtures(project(":ckc-core")))
    testImplementation(kotlin("test-junit5"))
    testImplementation("io.micrometer:micrometer-core:1.15.0")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.9.0")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.2")
    testImplementation("org.mockito.kotlin:mockito-kotlin:5.3.1")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter:2.0.2")
    testImplementation("org.testcontainers:testcontainers-kafka:2.0.2")
}

val brokerCliDirectory = layout.buildDirectory.dir("warmup-broker-cli")
val stageWarmupBrokerCli = tasks.register<Sync>("stageWarmupBrokerCli") {
    from(tasks.jar, brokerCliRuntime)
    into(brokerCliDirectory)
}

tasks.test {
    useJUnitPlatform {
        excludeTags("integration")
    }

    testLogging {
        events("PASSED", "FAILED", "SKIPPED")
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }
}

val integrationTest = tasks.register<Test>("integrationTest") {
    description = "Runs integration tests against real dependencies"
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    dependsOn(stageWarmupBrokerCli)
    inputs.dir(brokerCliDirectory)
    systemProperty("ckc.warmup.brokerCliDirectory", brokerCliDirectory.get().asFile.absolutePath)
    shouldRunAfter(tasks.test)
    useJUnitPlatform {
        includeTags("integration")
    }

    testLogging {
        events("PASSED", "FAILED", "SKIPPED")
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }
}

val testRuntimeWithoutMicrometer = configurations.resolvable("testRuntimeWithoutMicrometer") {
    val testRuntime = configurations.testRuntimeClasspath.get()
    extendsFrom(testRuntime)
    attributes {
        fun <T : Any> copyAttribute(key: Attribute<T>) {
            attribute(key, requireNotNull(testRuntime.attributes.getAttribute(key)))
        }
        testRuntime.attributes.keySet().forEach { copyAttribute(it) }
    }
    exclude(group = "io.micrometer")
}

val integrationTestWithoutMicrometer = tasks.register<Test>("integrationTestWithoutMicrometer") {
    description = "Runs Kafka warm-up scenarios without the Micrometer libraries"
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = files(sourceSets.test.get().output, sourceSets.main.get().output, testRuntimeWithoutMicrometer)
    // JUnit resolves candidate classes before applying tags. Adapter-specific tests require Micrometer.
    exclude("avh/ckc/warmup/micrometer/**")
    shouldRunAfter(tasks.test, integrationTest)
    useJUnitPlatform {
        includeTags("without-micrometer")
    }
    testLogging {
        events("PASSED", "FAILED", "SKIPPED")
    }
}

tasks.check {
    dependsOn(integrationTest, integrationTestWithoutMicrometer)
}
