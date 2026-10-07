# Apache Kafka Notes

## Core Concepts

### 1. Producer

Sends data (messages) to Kafka. Publishes messages to a topic, and can choose which partition a message goes to (by key or round-robin). A producer doesn't know which consumer will eventually read the data — it just publishes.

### 2. Topic

A logical category or stream of data. Messages published to a topic are stored inside partitions.

### 3. Partition

The physical storage unit of a topic. Partitions provide parallelism, and messages inside a single partition are always kept in order.

### 4. Consumer

Reads messages from topics. Consumers are pull-based, meaning the consumer asks Kafka for data rather than Kafka pushing it. Every consumer belongs to a consumer group.

### 5. Consumer Group ⭐

A group of consumers working together to read from a topic. For a given group, each partition is read by only one consumer in that group (so partitions are divided among the group's consumers, not duplicated).

Enables:
- **Scalability** - add more consumers to read more partitions in parallel
- **Load balancing** - work is spread across the group's consumers
- **Fault tolerance** - if one consumer dies, its partitions are reassigned to the rest of the group

### 6. Broker

A Kafka server that stores data and handles producer and consumer requests. A **cluster** is made up of multiple brokers working together.

---

## Architecture Overview

### Publish / Subscribe Flow

Producers (**Pubs**) publish to a topic's partitions inside the Kafka broker. Each **consumer group** then independently reads from those same partitions — every group gets its own copy of the data, while consumers *within* a group split the partitions between them.

```
Pubs --> Kafka Broker
              |
         +----+----+
         |         |
      Topic 1   Topic 2
      (p1,p2,p3) (p1,p2,p3)
         |
   -------------------------------
   |                             |
Consumer Group 1           Consumer Group 2
(c1, c2, c3)                (c1, c2, c3, c4)
```
Each partition in a topic is matched to exactly one consumer within a given group — but the same partition can be read independently by a consumer in a different group.

### Kafka Cluster (Multiple Brokers)

A cluster distributes the same topic's partitions across multiple brokers, so no single broker holds all the data.

```
Kafka Cluster
├── Kafka Broker-0
│     └── Kafka Topic: P1, P2, P3
├── Kafka Broker-1
│     └── Kafka Topic: P1, P2, P3
└── Kafka Broker-2
      └── Kafka Topic: P1, P2, P3
```

---

## ZooKeeper vs KRaft Mode

Kafka needs something to manage **cluster coordination** — broker metadata, leader election, the controller node, and health monitoring.

### Old Kafka (with ZooKeeper) — Pre Kafka 2.8

An external **ZooKeeper** service acts as the coordinator for the brokers.

```
ZooKeeper (Coordinator)
├── Broker Metadata
├── Leader Election
├── Controller Node
└── Health Monitoring
        |
   -----------------
   |       |       |
Broker B1 Broker B2 Broker B3
   |       |       |
   --------+--------
            |
   Kafka Topics & Messages
```

### Modern Kafka (KRaft Mode) — Kafka 2.8+

ZooKeeper is replaced by an internal **Kafka Controller**, so there's no external dependency.

```
Kafka Controller
   |
   -----------------
   |       |       |
Broker B1 Broker B2 Broker B3
   |       |       |
   --------+--------
            |
   Built-In Metadata
   Internal Controller
   Leader Election
   No ZooKeeper Needed
            |
   Kafka Topics & Messages
```

| | ZooKeeper (old) | KRaft Mode (new) |
|---|---|---|
| Coordination | External ZooKeeper service | Internal Kafka Controller |
| Dependency | Needs a separate ZooKeeper cluster | ZooKeeper-free |
| Metadata | Stored in ZooKeeper | Built-in |

---

## Setup & Running Kafka (Windows)

Copy the server and zookeeper properties files from `config` into the `windows` folder, then open Command Prompt as Administrator.

Start ZooKeeper:
```bash
zookeeper-server-start.bat zookeeper.properties
```

Start the Kafka server:
```bash
kafka-server-start.bat server.properties
```

### Running Multiple Brokers

To run multiple brokers on the same machine, make another copy of the server folder/config and rename it, then update these three properties so it doesn't clash with the first broker:
```properties
broker.id=1
listeners=PLAINTEXT://:9093
log.dirs=/tmp/kafka-logs2
```

---

## Topic Commands

To create a topic:
```bash
kafka-topics.bat --create --topic telusko --bootstrap-server localhost:9092 --partitions 4 --replication-factor 2
```
Replication factor is set to 2 since two brokers were created.

To check how many partitions and replicas were created:
```bash
kafka-topics.bat --describe --topic telusko --bootstrap-server localhost:9092
```

To check the list of topics created:
```bash
kafka-topics.bat --list --bootstrap-server localhost:9092
```

### Example Output

```
D:\kafka_2.13-3.9.0\bin\windows>kafka-topics.bat --create --topic telusko --bootstrap-server localhost:9092 --partitions 4 --replication-factor 2
Created topic telusko.

D:\kafka_2.13-3.9.0\bin\windows>kafka-topics.bat --describe --topic telusko --bootstrap-server localhost:9092
Topic: telusko  TopicId: 12A5c8yPS1CGfsE5iPv-PA  PartitionCount: 4  ReplicationFactor: 2  Configs:
        Topic: telusko  Partition: 0  Leader: 1  Replicas: 1,0  Isr: 1,0  Elr: N/A  LastKnownElr: N/A
        Topic: telusko  Partition: 1  Leader: 0  Replicas: 0,1  Isr: 0,1  Elr: N/A  LastKnownElr: N/A
        Topic: telusko  Partition: 2  Leader: 1  Replicas: 1,0  Isr: 1,0  Elr: N/A  LastKnownElr: N/A
        Topic: telusko  Partition: 3  Leader: 0  Replicas: 0,1  Isr: 0,1  Elr: N/A  LastKnownElr: N/A

D:\kafka_2.13-3.9.0\bin\windows>kafka-topics.bat --list --bootstrap-server localhost:9092
telusko
```
Each partition has a **Leader** (the broker handling reads/writes for that partition) and **Replicas** (the brokers holding a copy). **Isr** (In-Sync Replicas) are the replicas currently fully caught up with the leader.

---

## Listing & Deleting Topics and Consumer Groups

To list all topics:
```bash
kafka-topics.bat --bootstrap-server localhost:9092 --list
```

To list all consumer groups:
```bash
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --list
```

To delete a topic:
```bash
kafka-topics.bat --bootstrap-server localhost:9092 --delete --topic [topic-name]
```

To delete a consumer group:
```bash
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --delete --group [group-name]
```
