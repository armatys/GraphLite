plugins {
    id("com.android.library")
    id("org.jetbrains.kotlin.multiplatform")
    id("maven-publish")
}

android {
    compileSdk = 30

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }

    defaultConfig {
        minSdk = 21
        targetSdk = 30
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        testInstrumentationRunnerArguments["clearPackageData"] = "true"
    }

    externalNativeBuild {
        ndkBuild {
            path(file("src/androidMain/jni/Android.mk"))
        }
    }

    ndkVersion = "23.0.7599858"

    packagingOptions {
        resources {
            excludes.add("META-INF/AL2.0")
            excludes.add("META-INF/LGPL2.1")
        }
    }

    sourceSets {
        getByName("androidTest") {
            java.srcDir("src/androidAndroidTest/kotlin")
            manifest.srcFile("src/androidAndroidTest/AndroidManifest.xml")
        }
        getByName("main") {
            java.srcDir("src/androidMain/java")
            manifest.srcFile("src/androidMain/AndroidManifest.xml")
        }
    }
}

repositories {
    mavenCentral()
    google()
}

group = "pl.makenika.graphlite"
version = "0.1.0"

kotlin {
    explicitApi()

    android {
        compilations.all {
            kotlinOptions {
                jvmTarget = "1.8"
            }
        }
        publishLibraryVariants("release")
    }

    jvm {
        compilations.all {
            kotlinOptions {
                jvmTarget = "1.8"
            }
        }
    }

    targets.all {
        compilations.all {
            kotlinOptions {
                allWarningsAsErrors = true
            }
        }
    }

    sourceSets {
        val androidAndroidTestRelease by getting
        val androidAndroidTest by getting {
            dependsOn(androidAndroidTestRelease) // workaround for a warning
            dependencies {
                implementation(libs.androidxTest.core)
                implementation(libs.androidxTest.runner)
                implementation(libs.androidxTest.rules)
                implementation(libs.androidxTestExt.junit)
                implementation(libs.kotlinTest.junit)
            }
        }
        val androidMain by getting

        val commonMain by getting {
            dependencies {
                implementation(libs.uuid)
                api(libs.coroutines.core)
            }
        }
        val commonTest by getting {
            dependencies {
                implementation(libs.bundles.kotlinTestCommon)
                implementation(libs.coroutines.test)
            }
        }

        val jvmMain by getting {
            dependencies {
                implementation(libs.sqlite.jdbc)
            }
        }
        val jvmTest by getting {
            dependencies {
                implementation(libs.kotlinTest.junit)
            }
        }
    }
}

publishing {
    repositories {
        maven {
            name = "GitHubPackages"
            credentials {
                username = System.getenv("USERNAME")
                password = System.getenv("TOKEN")
            }
            url = uri("https://maven.pkg.github.com/armatys/GraphLite")
        }
    }
}
