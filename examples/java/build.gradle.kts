plugins {
    application
    java
}

dependencies {
    implementation("xyz.triplox:triplox:0.1.0-alpha.3")
    implementation("org.clojure:clojure:1.12.3")
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

application {
    mainClass.set("SimpleExample")
}
