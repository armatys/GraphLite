buildscript {
    repositories {
        mavenCentral()
        google()
    }
    dependencies {
        classpath("com.android.tools.build:gradle:7.0.2")
        classpath(libs.kotlin.gradle)
        classpath(libs.androidxBenchmark.gradle)
    }
}
