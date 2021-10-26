enableFeaturePreview("VERSION_CATALOGS")

dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            version("coroutines", "1.5.2-native-mt")
            version("kotlin", "1.5.31")
            version("androidxBenchmark", "1.0.0")
            version("androidxTest", "1.4.0")

            alias("androidxBenchmark-gradle")
                .to("androidx.benchmark", "benchmark-gradle-plugin")
                .versionRef("androidxBenchmark")

            alias("androidxBenchmark-junit")
                .to("androidx.benchmark", "benchmark-junit4")
                .versionRef("androidxBenchmark")

            alias("androidxTest-core")
                .to("androidx.test", "core")
                .versionRef("androidxTest")

            alias("androidxTest-rules")
                .to("androidx.test", "rules")
                .versionRef("androidxTest")

            alias("androidxTest-runner")
                .to("androidx.test", "runner")
                .versionRef("androidxTest")

            alias("androidxTestExt-junit")
                .to("androidx.test.ext", "junit")
                .version("1.1.3")

            alias("coroutines-core")
                .to("org.jetbrains.kotlinx", "kotlinx-coroutines-core")
                .versionRef("coroutines")

            alias("coroutines-test")
                .to("org.jetbrains.kotlinx", "kotlinx-coroutines-test")
                .versionRef("coroutines")

            alias("junit")
                .to("junit", "junit")
                .version("4.13.2")

            alias("kotlin-gradle")
                .to("org.jetbrains.kotlin", "kotlin-gradle-plugin")
                .versionRef("kotlin")

            alias("kotlinTest-annotationsCommon")
                .to("org.jetbrains.kotlin", "kotlin-test-annotations-common")
                .versionRef("kotlin")

            alias("kotlinTest-common")
                .to("org.jetbrains.kotlin", "kotlin-test-common")
                .versionRef("kotlin")

            alias("kotlinTest-junit")
                .to("org.jetbrains.kotlin", "kotlin-test-junit")
                .versionRef("kotlin")

            alias("sqlite-jdbc")
                .to("org.xerial", "sqlite-jdbc")
                .version("3.36.0")

            alias("uuid")
                .to("com.benasher44", "uuid")
                .version("0.1.0")

            bundle(
                "kotlinTestCommon",
                listOf("kotlinTest-annotationsCommon", "kotlinTest-common")
            )
        }
    }
}

rootProject.name = "graphlite"
include(":benchmark")
include(":graphlite")
