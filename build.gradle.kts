plugins {
    id("java")
}

group = "io.conduktor.demo"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {

    // Source: https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
    implementation("org.apache.kafka:kafka-clients:4.3.1")

    // Source: https://mvnrepository.com/artifact/org.slf4j/slf4j-api
    implementation("org.slf4j:slf4j-api:2.0.20")

    // Source: https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
    implementation("org.slf4j:slf4j-simple:2.0.20")

//    // Source: https://mvnrepository.com/artifact/com.squareup.okhttp/okhttp
//    implementation("com.squareup.okhttp:okhttp:2.7.5")

    // Source: https://mvnrepository.com/artifact/com.launchdarkly/okhttp-eventsource
    implementation("com.launchdarkly:okhttp-eventsource:5.0.0")
}

tasks.test {
    useJUnitPlatform()
}