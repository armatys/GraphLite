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

    ndkVersion = "21.3.6528147"

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
            java.srcDir("src/androidMain/kotlin")
            manifest.srcFile("src/androidMain/AndroidManifest.xml")
        }
    }
}

repositories {
    mavenCentral()
    google()
}

group = "pl.makenika.graphlite"
version = "0.0.12"

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

    sourceSets["androidMain"].dependencies {
    }

    sourceSets["androidAndroidTest"].dependencies {
        implementation("androidx.benchmark:benchmark-junit4:1.0.0")
        implementation("androidx.test:core:1.4.0")
        implementation("androidx.test:runner:1.4.0")
        implementation("androidx.test:rules:1.4.0")
        implementation("androidx.test.ext:junit:1.1.3")
        implementation("org.jetbrains.kotlin:kotlin-test-junit")
    }

    sourceSets["commonMain"].dependencies {
        implementation("com.benasher44:uuid:0.1.0")
        api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.2-native-mt")
    }

    sourceSets["commonTest"].dependencies {
        implementation("org.jetbrains.kotlin:kotlin-test-common")
        implementation("org.jetbrains.kotlin:kotlin-test-annotations-common")
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.5.2-native-mt")
    }

    sourceSets["jvmMain"].dependencies {
        implementation("org.xerial:sqlite-jdbc:3.36.0")
    }

    sourceSets["jvmTest"].dependencies {
        implementation("org.jetbrains.kotlin:kotlin-test-junit")
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
