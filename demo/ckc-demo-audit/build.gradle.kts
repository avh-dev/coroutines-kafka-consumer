import org.gradle.api.JavaVersion
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm")
}

description = "Shared file-backed audit writer for demo load-test runs"

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

val junitVersion = "5.10.2"

dependencies {
    implementation(kotlin("stdlib"))

    testImplementation(kotlin("test-junit5"))
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    constraints {
        testImplementation("org.junit.jupiter:junit-jupiter-api") {
            version { require(junitVersion) }
        }
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine") {
            version { require(junitVersion) }
        }
    }
}

tasks.test {
    useJUnitPlatform()
}
