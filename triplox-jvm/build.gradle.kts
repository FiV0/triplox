plugins {
    `java-library`
    `maven-publish`
    id("dev.clojurephant.clojure") version "0.8.0-beta.7"
    id("com.vanniktech.maven.publish") version "0.34.0"
}

repositories {
    mavenCentral()
    maven { url = uri("https://repo.clojars.org/") }
}

fun workspaceVersion(): String {
    var inWorkspacePackage = false
    for (line in rootProject.projectDir.parentFile.resolve("Cargo.toml").readLines()) {
        val trimmed = line.trim()
        when {
            trimmed == "[workspace.package]" -> inWorkspacePackage = true
            inWorkspacePackage && trimmed.startsWith("[") -> inWorkspacePackage = false
            inWorkspacePackage && trimmed.startsWith("version = ") -> {
                return trimmed.substringAfter('"').substringBefore('"')
            }
        }
    }
    error("Could not read workspace package version from Cargo.toml")
}

val triploxVersion = (findProperty("triploxVersion") as String?) ?: workspaceVersion()

dependencies {
    // Clojure
    // TODO Keyword currently leaks into the public API
    api("org.clojure", "clojure", "1.12.3")
    implementation("org.clojure", "core.async", "1.9.865")

    // MessagePack wire codec
    implementation("org.msgpack:msgpack-core:0.9.8")
    implementation("com.squareup.okhttp3:okhttp:4.12.0")

    // logging
    implementation("org.clojure", "tools.logging", "1.3.0")
    implementation("ch.qos.logback", "logback-classic", "1.4.5")

    // test
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.9.0")
    testRuntimeOnly("dev.clojurephant", "jovial", "0.4.1")

    // dev
    nrepl("cider", "cider-nrepl", "0.58.0")
}

tasks.test {
    useJUnitPlatform {
        excludeTags("integration")
    }
    exclude("**/integration/**")
}

tasks.register<Test>("integrationTest") {
    useJUnitPlatform()
    include("**/integration/**", "**/Integration*")
    systemProperty("triplox.host", System.getenv("TRIPLOX_HOST") ?: "localhost")
    systemProperty("triplox.port", System.getenv("TRIPLOX_PORT") ?: "5490")
}

tasks.clojureRepl {
    classpath = files(classpath, sourceSets["main"].output)
    middleware.add("cider.nrepl/cider-middleware")
}

tasks.checkClojure {
    enabled = false
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

mavenPublishing {
    publishToMavenCentral()

    coordinates("xyz.triplox", "triplox", triploxVersion)

    pom {
        name.set("triplox")
        description.set("A Datomic-like triplestore in Rust on top of SlateDB")
        url.set("https://github.com/FiV0/triplox")

        licenses {
            license {
                name.set("Apache License, Version 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0")
            }
        }

        developers {
            developer {
                id.set("FiV0")
                name.set("Finn Völkel")
            }
        }

        scm {
            url.set("https://github.com/FiV0/triplox")
            connection.set("scm:git:git://github.com/FiV0/triplox.git")
            developerConnection.set("scm:git:ssh://git@github.com/FiV0/triplox.git")
        }
    }
}
