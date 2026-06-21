# Pub/Sub Theory & Practical — Build Messaging Systems from First Principles

**Pub/Sub stands for Publisher / Subscriber**

**Pub/Sub Theory & Practical** is a beginner-friendly repository that explains how modern messaging systems work by combining **theory with clean Python implementations**.

Instead of treating concepts like **brokers, topics, partitions, offsets, producers, consumers, and replication** as interview buzzwords, this project builds them from scratch and explains *why they exist* and *how they interact*. The goal is to provide an intuitive mental model that makes understanding systems like **Apache Kafka**, **RabbitMQ**, **Google Pub/Sub**, and cloud event buses much easier.

Whether you're preparing for placements, backend interviews, or learning distributed systems, this repository serves as a compact reference that you can revisit anytime.

---

# System Architecture

## Push-Based Pub/Sub

```text
Publisher
    |
    v
+-----------+
|  Broker   |
+-----------+
    |
    v
+-----------+
|   Topic   |
+-----------+
    |
    +-----------> Subscriber A
    |
    +-----------> Subscriber B
```

The broker immediately pushes every published message to all subscribed consumers.

---

## Pull-Based Pub/Sub (Kafka Style)

```text
Producer
    |
    v
+-----------+
|  Broker   |
+-----------+
    |
    v
+-----------+
|   Topic   |
+-----------+
    |
    +-----------------------------+
    |             |               |
    v             v               v
Partition 0   Partition 1   Partition 2
    |             |               |
    +-------------+---------------+
                  |
                  v
        Consumers Poll Using Offsets
```

Messages are stored in partitions, and consumers fetch them independently while tracking their own offsets.

---

# ✨ Key Features

- **Push-Based Pub/Sub Implementation** — Demonstrates immediate message delivery from broker to subscribers.
- **Pull-Based (Kafka-Inspired) Implementation** — Simulates producers, consumers, partitions, and offsets.
- **Partitioning Logic** — Understand how hashing distributes messages for scalability.
- **Offset Tracking** — Learn how consumers resume processing without rereading old messages.
- **Conceptual Explanations** — Covers brokers, topics, keys, partitions, replication, and routing.
- **Minimal Python Code** — Easy to read, modify, and extend for learning purposes.
- **Interview-Oriented** — Useful for system design rounds and backend engineering preparation.

---

# Project Structure

```text
PUB_SUB/
├── Push_Based_pub_sub.py
├── Pull_Based_pub_sub.py
└── README.md
```

Both files are self-contained and include executable examples using:

```python
if __name__ == "__main__":
```

making them easy to experiment with.

---

# Quick Start

## 1. Clone the Repository

```bash
git clone https://github.com/yourusername/pub-sub-theory-practical.git
cd pub-sub-theory-practical
```

## 2. Run the Push-Based Example

```bash
python Push_Based_pub_sub.py
```

## 3. Run the Pull-Based Example

```bash
python Pull_Based_pub_sub.py
```

No external dependencies are required.

---

# Technology Stack

| Component | Technology |
| ---------- | ---------- |
| Language | Python |
| Paradigm | Object-Oriented Programming |
| Architecture | Event-Driven Systems |
| Concepts | Pub/Sub, Producers, Consumers, Brokers |
| Scaling | Partitions, Hashing |
| Reliability | Offsets, Replication Theory |

---

# How It Works

## 1. Publishing

A producer (or publisher) creates a message and sends it to a broker.

```
Producer
    |
    v
Broker
```

---

## 2. Topic Routing

The broker identifies the destination topic.

```
Broker
   |
   v
Topic
```

In pull-based systems, a **key** can also be supplied.

Example:

```
key = "driver_42"
```

---

## 3. Partition Selection

The key is hashed to consistently route related messages into the same partition.

```
partition = hash(key) % number_of_partitions
```

```
key
 |
 v
hash()
 |
 v
Partition 3
```

This preserves ordering for messages sharing the same key while distributing load across partitions.

---

## 4. Consumption

### Push Model

```
Broker
   |
   +------> Subscriber A
   |
   +------> Subscriber B
```

Subscribers receive messages immediately.

### Pull Model

```
Consumer
     |
     | poll()
     v
Partition Log
```

Consumers fetch unread messages whenever they choose.

---

## 5. Offsets

Each partition maintains an append-only log.

```
Partition 1

Offset 0 -> Ping A
Offset 1 -> Ping B
Offset 2 -> Ping C
```

Consumers remember their current offset and continue reading from there during the next poll.

---

# Core Concepts

## Topics

A **Topic** is a logical category for messages.

Examples:

- `driver_locations`
- `payments`
- `orders`
- `notifications`

---

## Partitions

Each topic can be split into multiple partitions.

```
orders

├── Partition 0
├── Partition 1
└── Partition 2
```

Partitioning improves throughput and enables horizontal scaling.

---

## Brokers

A **Broker** stores partitions and routes messages.

```
Broker A
    ├── orders (Partition 0)
    └── payments (Partition 1)

Broker B
    ├── orders (Partition 1)
    └── payments (Partition 0)
```

A broker does **not** own every partition—only the ones assigned to it.

---

## Keys

Keys determine partition placement.

```
driver_123
     |
     v
hash(driver_123)
     |
     v
Partition 5
```

Messages with the same key always land in the same partition.

---

## Offsets

Offsets uniquely identify a message's position within a partition.

```
Offset 0
Offset 1
Offset 2
Offset 3
```

Consumers use offsets to avoid rereading old messages.

---

## Replication

Replication creates backup copies of partitions across brokers.

```
Partition 5

Leader  ---> Broker A
Replica ---> Broker B
Replica ---> Broker C
```

If the leader fails, another replica can be promoted.

---

# Push vs Pull

| Push-Based | Pull-Based |
|------------|-----------|
| Broker sends messages immediately | Consumer requests messages |
| Simpler design | Better scalability |
| Usually no offsets | Uses offsets extensively |
| Great for notifications | Great for streaming pipelines |
| Real-time callbacks | Durable logs and replay |

---

# Partitioning vs Replication

These solve different problems.

| Partitioning | Replication |
|--------------|-------------|
| Scales throughput | Increases fault tolerance |
| Splits data | Copies data |
| Uses hashing | Uses synchronization |
| Improves parallelism | Prevents data loss |

---

# Real-World Technologies

| Technology | Model | Notes |
|-----------|-------|------|
| Apache Kafka | Pull | Partitioned logs with consumer-managed offsets |
| RabbitMQ | Mostly Push | Queue-based broker supporting multiple messaging patterns |
| Google Cloud Pub/Sub | Pull & Push | Supports subscribers polling or receiving pushed messages |
| Apache Pulsar | Primarily Pull | Distributed messaging with partitions and durable storage |
| Amazon Kinesis | Pull | Streaming platform with shard-based consumption |
| Redis Pub/Sub | Push | Lightweight in-memory publish/subscribe with no durable history |
| NATS | Push | High-performance messaging for microservices and event-driven systems |

---

# Why This Repository?

This project focuses on **understanding before memorization**.

By reading the theory and experimenting with the implementations, you'll develop intuition for:

- Object-oriented design
- Event-driven architectures
- Message brokers
- Topic management
- Partitioning strategies
- Hash-based routing
- Offset management
- Producer/consumer workflows
- Replication fundamentals
- Scalable distributed systems

---

# ❤️ Made With Passion

Made with ❤️ by **Divi Jaiwanth**

If this repository helped you understand distributed systems or prepare for interviews, consider giving it a ⭐ and sharing it with others learning backend engineering.