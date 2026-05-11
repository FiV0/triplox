plugins {
    `java-library`
    `maven-publish`
    id("dev.clojurephant.clojure") version "0.8.0-beta.7"
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
    implementation("org.clojure", "clojure", "1.12.3")

    // MessagePack wire codec
    implementation("org.msgpack:msgpack-core:0.9.8")

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

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "xyz.triplox"
            artifactId = "triplox"
            version = triploxVersion

            from(components["java"])

            pom {
                name.set("triplox")
                description.set("A triple store built on top of XTDB")
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
    }

    repositories {
        maven {
            name = "clojars"
            url = uri("https://clojars.org/repo")
            credentials {
                username = findProperty("clojarsUsername") as String?
                password = findProperty("clojarsPassword") as String?
            }
        }
    }
}
