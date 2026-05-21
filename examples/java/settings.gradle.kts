pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
    }
}

rootProject.name = "triplox-java-example"

includeBuild("../../triplox-jvm") {
    dependencySubstitution {
        substitute(module("xyz.triplox:triplox")).using(project(":"))
    }
}
