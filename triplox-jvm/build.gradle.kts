plugins {
    `java-library`
    id("dev.clojurephant.clojure") version "0.8.0-beta.7"
}

repositories {
    mavenCentral()
    maven { url = uri("https://repo.clojars.org/") }
}

dependencies {
    // Clojure
    implementation("org.clojure", "clojure", "1.12.3")

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
