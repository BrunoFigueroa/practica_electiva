# Distributed MAPE-K Experimental Prototype

This repository contains a reference implementation developed for an academic project to evaluate different MAPE-K architectures under controlled conditions.  
The goal is to compare centralized, functionally distributed, and ring-based MAPE models using a synthetic workload.

This is a **prototype for experimental purposes**, not a production-ready system.

---

## Project Structure

- `flink-job/`  
  Flink job that computes the n-th prime number (synthetic CPU-intensive workload).

- `centralized-mape/`  
  Centralized MAPE-K implementation.

- `mape-*/`  
  Functionally distributed MAPE-K implementation.

- `mape-ring/`  
  Ring-based (peer-to-peer) MAPE-K implementation.

---

## Execution Instructions

### 1. Create Kafka Topics

Kafka topics must be created manually before starting the system.

```bash
docker run --rm -it --network=codigo_default apache/kafka:3.7.0 \
  /opt/kafka/bin/kafka-topics.sh --create \
  --topic input \
  --bootstrap-server codigo-kafka-1:9092 \
  --partitions 3 \
  --replication-factor 1
```

```bash
docker run --rm -it --network=codigo_default apache/kafka:3.7.0 \
  /opt/kafka/bin/kafka-topics.sh --create \
  --topic output \
  --bootstrap-server codigo-kafka-1:9092 \
  --partitions 3 \
  --replication-factor 1
```

---

### 2. Build the Flink Job

Using **Gradle or Maven**, compile the file:

```
flink-job/src/main/scala/NthPrimeJob.scala
```

The generated `.jar` file must be copied to the following locations:

```
centralized-mape/app.jar
mape-E/app.jar
mape-ring/app.jar
```

This allows each MAPE model to deploy the same Flink job used during evaluation.

---

### 3. Flink Job Dependencies

The Flink job uses the following dependencies:

```gradle
implementation "org.scala-lang:scala-library:2.12.15"
implementation "org.apache.flink:flink-streaming-scala_2.12:1.18.1"
implementation "org.apache.flink:flink-clients:1.18.1"
implementation "org.apache.flink:flink-connector-kafka:3.1.0-1.18"
```

---

### 4. Start the System

Once the job has been compiled and the JAR files are in place, start all components using Docker Compose:

```bash
docker compose pull
docker compose up -d --build
```

The Flink Web UI is available at:

```
http://localhost:8081
```

## MAPE Architectures Selection

Due to the experimental nature of this project, **not all MAPE architectures can be executed simultaneously**.

The `docker-compose.yml` file includes services for:
- Centralized MAPE
- Functionally distributed MAPE (M / A / P / E)
- Ring-based MAPE

Only **one MAPE architecture should be active at a time**.

To select a specific architecture:
1. Comment out the MAPE services you do **not** want to use.
2. Uncomment the services corresponding to the desired MAPE model.
3. Restart Docker Compose.

Example:
- To run the **ring-based MAPE**, enable only `mape-ring-*` services.
- To run the **centralized MAPE**, enable only `mape-centralized`.
- To run the **functionally distributed MAPE**, enable `mape-M`, `mape-A`, `mape-P`, and `mape-E`.


