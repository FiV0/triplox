import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.Base64

plugins {
    `java-library`
    `maven-publish`
    signing
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

fun propertyOrEnv(propertyName: String, envName: String): String? =
    (findProperty(propertyName) as String?) ?: System.getenv(envName)

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

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(21))
    withSourcesJar()
    withJavadocJar()
}

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
                username = propertyOrEnv("clojarsUsername", "CLOJARS_USERNAME")
                password = propertyOrEnv("clojarsPassword", "CLOJARS_PASSWORD")
            }
        }

        maven {
            name = "central"
            url = uri("https://ossrh-staging-api.central.sonatype.com/service/local/staging/deploy/maven2/")
            credentials {
                username = propertyOrEnv("centralUsername", "CENTRAL_USERNAME")
                password = propertyOrEnv("centralPassword", "CENTRAL_PASSWORD")
            }
        }
    }
}

signing {
    setRequired {
        gradle.taskGraph.hasTask(":publishMavenPublicationToCentralRepository") ||
            gradle.taskGraph.hasTask(":publishMavenPublicationToCentralPortal")
    }

    val signingKey = propertyOrEnv("signingInMemoryKey", "SIGNING_KEY")
    val signingPassword = propertyOrEnv("signingInMemoryKeyPassword", "SIGNING_PASSWORD")
    if (signingKey != null) {
        useInMemoryPgpKeys(signingKey, signingPassword)
    }
    sign(publishing.publications["maven"])
}

tasks.register("uploadCentralDeployment") {
    group = "publishing"
    description = "Uploads the Central OSSRH compatibility staging repository to the Central Portal."
    dependsOn("publishMavenPublicationToCentralRepository")

    doLast {
        val username = propertyOrEnv("centralUsername", "CENTRAL_USERNAME")
            ?: throw GradleException("Missing centralUsername property or CENTRAL_USERNAME environment variable")
        val password = propertyOrEnv("centralPassword", "CENTRAL_PASSWORD")
            ?: throw GradleException("Missing centralPassword property or CENTRAL_PASSWORD environment variable")
        val namespace = propertyOrEnv("centralNamespace", "CENTRAL_NAMESPACE") ?: "xyz.triplox"
        val publishingType = propertyOrEnv("centralPublishingType", "CENTRAL_PUBLISHING_TYPE") ?: "user_managed"
        val auth = Base64.getEncoder().encodeToString("$username:$password".toByteArray(Charsets.UTF_8))

        val request = HttpRequest.newBuilder(
            URI.create(
                "https://ossrh-staging-api.central.sonatype.com/manual/upload/defaultRepository/" +
                    "$namespace?publishing_type=$publishingType",
            ),
        )
            .header("Authorization", "Bearer $auth")
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() !in 200..299) {
            throw GradleException(
                "Central Portal upload failed with HTTP ${response.statusCode()}: ${response.body()}",
            )
        }
        logger.lifecycle("Uploaded Maven Central deployment for namespace $namespace using publishing_type=$publishingType")
    }
}

tasks.register("publishMavenPublicationToCentralPortal") {
    group = "publishing"
    description = "Publishes the JVM client to the Central Portal using the Central OSSRH compatibility API."
    dependsOn("uploadCentralDeployment")
}
