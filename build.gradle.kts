allprojects {
    group = "dev.avh"
    version = "0.0.1"

    repositories {
        mavenCentral()
    }
}

plugins {
    kotlin("jvm") version "2.0.0" apply false
    kotlin("plugin.serialization") version "2.0.0" apply false
    id("com.google.protobuf") version "0.9.4" apply false
    id("org.springframework.boot") version "3.3.5" apply false
}
