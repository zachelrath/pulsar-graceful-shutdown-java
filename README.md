# Graceful shutdown with Apache Pulsar Java client in Spring Boot

Sample application created to demonstrate how to properly implement graceful shutdown of Apache Pulsar queue consumers in Java with Spring Boot, to accompany this blog post:

https://medium.com/@zachcorbettmcelrath/graceful-shutdown-of-pulsar-queue-consumers-in-java-and-spring-boot-f93645a92b2b

### Run Pulsar (in standalone cluster, via Docker Compose)

```shell
docker compose up -d
```

### Build the app

```shell
./mvnw clean install
```

### Run producer and consumer (via command-line)

#### Producer

```shell
SPRING_PROFILES_ACTIVE=producer ./mvnw spring-boot:run
```

#### Consumer

```shell
SPRING_PROFILES_ACTIVE=consumer ./mvnw spring-boot:run
```
