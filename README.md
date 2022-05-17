# Graceful shutdown with Apache Pulsar Java client in Spring Boot

### Run Pulsar (in standalone cluster, via Docker Compose)

```shell
docker compose up -d
```

### Build the app

```shell
./mvnw clean install
```

### Run producer and consumner

#### Producer

```shell
SPRING_PROFILES_ACTIVE=producer ./mvnw spring-boot:run
```

#### Consumer

```shell
SPRING_PROFILES_ACTIVE=consumer ./mvnw spring-boot:run
```
