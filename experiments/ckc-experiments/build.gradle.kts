val withExperiments = providers.gradleProperty("withExperiments").isPresent

tasks.configureEach {
    if (!withExperiments) {
        enabled = false
    }
}

plugins {
    kotlin("jvm")
    `java-test-fixtures`
    id("me.champeau.jmh") version "0.7.3"
}

kotlin {
    jvmToolchain(21)
}

dependencies {
    implementation(project(":ckc-core"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")

    testImplementation(kotlin("test-junit5"))
    testFixturesImplementation(kotlin("test-junit5"))

    testImplementation(testFixtures(project(":ckc-core")))

    val jmhVersion = "1.37"
    jmhImplementation("org.openjdk.jmh:jmh-core:$jmhVersion")
    jmhAnnotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:$jmhVersion")

    jmhImplementation(sourceSets.testFixtures.get().output)
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