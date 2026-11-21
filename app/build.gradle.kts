// Build configuration for NEXUS AI MAX.
plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
    id("org.jetbrains.kotlin.plugin.compose")
}
android {
    namespace = "ai.nexus.max"
    compileSdk = 35
    defaultConfig {
        applicationId = "ai.nexus.max"
        minSdk = 26
        targetSdk = 35
        versionCode = 4
        versionName = "0.4.0"
    }
    val signingStore = System.getenv("NEXUS_KEYSTORE_PATH")
    val signingStorePassword = System.getenv("NEXUS_KEYSTORE_PASSWORD")
    val signingKeyAlias = System.getenv("NEXUS_KEY_ALIAS")
    val signingKeyPassword = System.getenv("NEXUS_KEY_PASSWORD")

    if (!signingStore.isNullOrBlank() && !signingStorePassword.isNullOrBlank() &&
        !signingKeyAlias.isNullOrBlank() && !signingKeyPassword.isNullOrBlank()
    ) {
        signingConfigs {
            create("release") {
                storeFile = file(signingStore)
                storePassword = signingStorePassword
                keyAlias = signingKeyAlias
                keyPassword = signingKeyPassword
            }
        }
        buildTypes {
            getByName("release") {
                signingConfig = signingConfigs.getByName("release")
                isMinifyEnabled = false
            }
        }
    }

    buildFeatures { compose = true }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}
dependencies {
    implementation(platform("androidx.compose:compose-bom:2024.12.01"))
    implementation("androidx.activity:activity-compose:1.10.0")
    implementation("androidx.compose.ui:ui")
    implementation("androidx.compose.ui:ui-tooling-preview")
    implementation("androidx.compose.material3:material3")
    implementation("androidx.compose.material:material-icons-extended")
    implementation("androidx.lifecycle:lifecycle-viewmodel-compose:2.8.7")
    implementation("androidx.lifecycle:lifecycle-runtime-compose:2.8.7")
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    implementation("com.tom-roush:pdfbox-android:2.0.27.0")
    implementation("net.sf.kxml:kxml2:2.3.0")
    implementation("cz.adaptech.tesseract4android:tesseract4android:4.9.0")
    implementation("com.mikepenz:multiplatform-markdown-renderer-android:0.32.0")
    implementation("com.mikepenz:multiplatform-markdown-renderer-m3-android:0.32.0")
    implementation("com.mikepenz:multiplatform-markdown-renderer-code-android:0.32.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.9.0")
    testImplementation("junit:junit:4.13.2")
    testImplementation("org.json:json:20240303")
}
