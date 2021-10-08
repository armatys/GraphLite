plugins {
    id("com.android.library")
    id("androidx.benchmark")
    id("kotlin-android")
}

android {
    compileSdk = 30

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }

    kotlinOptions {
        jvmTarget = "1.8"
    }

    defaultConfig {
        minSdk = 21
        targetSdk = 30
        testInstrumentationRunner = "androidx.benchmark.junit4.AndroidBenchmarkRunner"
    }

    testBuildType = "release"
    buildTypes {
        getByName("debug") {
            // Since debuggable can"t be modified by gradle for library modules,
            // it must be done in a manifest - see src/androidTest/AndroidManifest.xml
            isMinifyEnabled = true
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "benchmark-proguard-rules.pro"
            )
        }
        getByName("release") {
            isDefault = true
        }
    }
}

repositories {
    mavenCentral()
    google()
}

dependencies {
    androidTestImplementation(libs.androidxBenchmark.junit)
    androidTestImplementation(libs.androidxTest.runner)
    androidTestImplementation(libs.androidxTestExt.junit)
    androidTestImplementation(libs.junit)

    // Add your dependencies here. Note that you cannot benchmark code
    // in an app module this way - you will need to move any code you
    // want to benchmark to a library module:
    // https://developer.android.com/studio/projects/android-library#Convert
    implementation(project(":graphlite"))
}