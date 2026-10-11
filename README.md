# Apache Kafka Notes

---

## 1. What Kafka Does

Kafka is a distributed event-streaming platform. Producers publish records to topics, Kafka stores them in partition logs, and consumers read them independently. Kafka only stores events; the business logic lives in the consumer applications.

```text
Orders Service --> Kafka topic: order-info
                        |                 |
                        v                 v
                Inventory Service   Notification Service
```

---

## 2. Core Concepts

### Producer

Sends records to Kafka and publishes them to a topic. A record has a key (optional), value, timestamp and headers. The producer never knows which consumer will read the data.

```json
{"orderId":203,"userId":7,"productName":"Laptop","quantity":1,"price":50000.0,"status":"CREATED"}
```

- **With a key:** the partitioner maps the key to a partition. The same key goes to the same partition as long as the partition count stays the same.
- **Without a key:** the producer's partitioner picks the partition. Don't assume it is always strict round-robin.

### Topic

A named logical stream of data, like `order-info`, `order-created` or `order-info-dlt`. Messages are stored inside its partitions. Reading a record does not delete it.

### Partition

The physical storage unit of a topic: an ordered, append-only log. Partitions give parallelism, and each record gets an **offset** inside its partition.

> Order is guaranteed **within** a partition, not across all partitions of a topic.

### Consumer

Reads messages from topics. It is **pull-based**: the consumer asks Kafka for data, Kafka does not push. It deserializes the bytes, runs the listener, and tracks its position with offsets. Every consumer belongs to a consumer group.

### Consumer Group ⭐

Consumers sharing the same `group.id` split the partitions between them. At any moment, a partition is assigned to **at most one consumer in that group**. Different groups read the same topic independently.

Example with a 4-partition topic:

| Consumers in the group | What happens |
|---|---|
| 1 | reads all 4 partitions |
| 2 | 2 partitions each |
| 4 | 1 partition each |
| 6 | 4 active, 2 idle |

Enables:
- **Scalability**: add consumers to read more partitions in parallel
- **Load balancing**: work is spread across the group
- **Fault tolerance**: if a consumer dies, Kafka rebalances its partitions to the others

### Broker and Cluster

A **broker** is one Kafka server that stores data and handles producer and consumer requests. A **cluster** is multiple brokers working together. Partitions and replicas are spread across brokers, so a broker does not necessarily hold every partition.

### Offset and Lag

- **Offset**: a record's position inside one partition (not global to the topic).
- **Committed offset**: the next position a group will read after a restart.
- **Log-end offset**: the position after the partition's latest record.
- **Lag** = log-end offset minus committed offset.

```text
Partition 0:  log-end offset = 120, committed offset = 100  -->  lag = 20
```

### Replication, Leader, Followers, ISR

| Term | Meaning |
|---|---|
| Leader | handles normal reads and writes for a partition |
| Follower | replica that copies the leader's records |
| Replication factor | total copies of a partition, including the leader |
| ISR | in-sync replicas, those caught up with the leader |
| Leader election | picks a new leader from eligible replicas when one fails |

Replication factor 2 needs 2 brokers, factor 3 needs 3.

> Replication protects **data**. Consumer groups distribute **processing**. They are different things.

### Retention

Kafka keeps records based on time or size policies. Offsets say where a group resumes, retention says how long records stay available.

---

## 3. Architecture

### Publish / Subscribe Flow

Producers publish to a topic's partitions inside the broker. Each consumer group reads independently, so every group gets its own copy of the data, while consumers inside a group split the partitions.

```text
Pubs --> Kafka Broker
              |
         +----+----+
         |         |
      Topic 1   Topic 2
      (p1,p2,p3) (p1,p2,p3)
         |
   ----------------------------
   |                          |
Consumer Group 1        Consumer Group 2
(c1, c2, c3)            (c1, c2, c3, c4)
```

Each partition goes to exactly one consumer inside a group, but the same partition can be read by a consumer in another group.

### Kafka Cluster (Multiple Brokers)

```text
Kafka Cluster
├── Kafka Broker-0  --> Topic partitions: P1, P2, P3
├── Kafka Broker-1  --> Topic partitions: P1, P2, P3
└── Kafka Broker-2  --> Topic partitions: P1, P2, P3
```

---

## 4. ZooKeeper vs KRaft

Kafka needs something to handle **cluster coordination**: broker metadata, leader election, the controller node and health monitoring.

```text
Old Kafka (ZooKeeper)               Modern Kafka (KRaft)

   ZooKeeper (external)               Kafka Controller (built in)
        |                                    |
  B1    B2    B3                       B1    B2    B3
        |                                    |
 Topics & Messages                    Topics & Messages
```

| | ZooKeeper mode | KRaft mode |
|---|---|---|
| Coordination | external ZooKeeper service | Kafka's own metadata quorum and controllers |
| Dependency | needs a separate ZooKeeper | ZooKeeper-free |
| Metadata | stored in ZooKeeper | built in |

Version notes: KRaft appeared in Kafka 2.8 (early access), became production-ready in 3.3, and Kafka 4.0 removed ZooKeeper mode completely.

> A Kafka 3.9.0 learning setup like the one below runs in **ZooKeeper mode**, so start ZooKeeper before the brokers. Don't mix ZooKeeper and KRaft configs.

---

## 5. Windows Setup

Assumed folder: `C:\kafka_2.13-3.9.0`. Open Command Prompt as Administrator and run everything from:

```cmd
cd C:\kafka_2.13-3.9.0\bin\windows
```

Start ZooKeeper (keep this terminal open):
```cmd
zookeeper-server-start.bat ..\..\config\zookeeper.properties
```

Start the broker in another terminal:
```cmd
kafka-server-start.bat ..\..\config\server.properties
```

If you copied the properties files into `bin\windows`, use those local names instead (`zookeeper.properties`, `server.properties`).

Check connectivity:
```cmd
kafka-broker-api-versions.bat --bootstrap-server localhost:9092
```

### Running Multiple Brokers

Make another copy of `server.properties`, rename it, and change these so it doesn't clash with broker 0:

```properties
broker.id=1
listeners=PLAINTEXT://:9093
advertised.listeners=PLAINTEXT://localhost:9093
log.dirs=C:/tmp/kafka-logs2
zookeeper.connect=localhost:2181
```

- Each broker needs a **unique ID, port and log directory**, but the same ZooKeeper.
- Never share a log directory between brokers.
- `advertised.listeners` matters because clients connect using the addresses in cluster metadata.

---

## 6. Topic and Console Commands

Create a topic (replication factor 2 since two brokers are running):
```cmd
kafka-topics.bat --bootstrap-server localhost:9092 --create --topic telusko --partitions 4 --replication-factor 2
```

On a three-broker cluster:
```cmd
kafka-topics.bat --bootstrap-server localhost:9092 --create --topic order-info --partitions 4 --replication-factor 3
```

List topics:
```cmd
kafka-topics.bat --bootstrap-server localhost:9092 --list
```

Check partitions and replicas:
```cmd
kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic telusko
```

```text
Topic: telusko  PartitionCount: 4  ReplicationFactor: 2
    Partition: 0  Leader: 1  Replicas: 1,0  Isr: 1,0
    Partition: 1  Leader: 0  Replicas: 0,1  Isr: 0,1
    Partition: 2  Leader: 1  Replicas: 1,0  Isr: 1,0
    Partition: 3  Leader: 0  Replicas: 0,1  Isr: 0,1
```
Read it like this: for partition 0, broker 1 is the leader, brokers 1 and 0 hold copies, and both are in sync. Leaders are spread across brokers.

Increase partitions (you can only increase, never reduce):
```cmd
kafka-topics.bat --bootstrap-server localhost:9092 --alter --topic order-info --partitions 6
```

Delete a topic:
```cmd
kafka-topics.bat --bootstrap-server localhost:9092 --delete --topic telusko
```

### Console Producer and Consumer

Send messages:
```cmd
kafka-console-producer.bat --bootstrap-server localhost:9092 --topic telusko
```

Read new messages:
```cmd
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic telusko
```

Read everything still retained, from the start:
```cmd
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic telusko --from-beginning
```

Produce with keys (then type `user-1:Hello from user 1`):
```cmd
kafka-console-producer.bat --bootstrap-server localhost:9092 --topic telusko --property parse.key=true --property key.separator=:
```

Show keys while consuming:
```cmd
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic telusko --from-beginning --property print.key=true --property key.separator=:
```

`Ctrl+C` stops a console process, it does not delete any records.

---

## 7. Consumer Groups and Offsets

List groups:
```cmd
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --list
```

Describe a group (shows committed offset, log-end offset, lag, consumer ID, host and client ID):
```cmd
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group inventory-group
```

Different groups have independent offsets. `inventory-group` and `notification-group` can both read `order-info` without affecting each other, while consumers inside `inventory-group` split the partitions.

### auto.offset.reset

Applies only when a group has **no valid committed offset**. It does not make an existing group replay from the start.

- `earliest`: start from the beginning of the available records
- `latest`: start from the end and read only new arrivals

### Reset Offsets

Stop the group's consumers first. The first command previews, the second executes (records will be read again):
```cmd
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group inventory-group --topic order-info --reset-offsets --to-earliest
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group inventory-group --topic order-info --reset-offsets --to-earliest --execute
```

### Delete a Group

Stop its consumers first. This does not delete topic records.
```cmd
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --delete --group inventory-test-group
```

> Never manually delete Kafka's internal `__consumer_offsets` topic.

---

## 8. Keys, Partitions and Parallelism

Pick a key based on the entity whose ordering matters:

| Key | Guarantees |
|---|---|
| `orderId` | ordering per order |
| `userId` | ordering per user |
| `productId` | ordering per product |

- Same key normally lands on the same partition, so its records stay in order.
- If the partition count changes, a key may map to a different partition.
- A 4-partition topic gives up to 4-way parallelism per consumer group. Extra consumers beyond 4 just sit idle.

---

## 9. Producer Reliability

### acks

| Setting | Meaning |
|---|---|
| `acks=0` | don't wait for any broker acknowledgement |
| `acks=1` | the leader acknowledges |
| `acks=all` | leader waits for the required in-sync replicas to acknowledge |

`acks=all` is the strongest, but real guarantees also depend on replication and ISR settings.

### min.insync.replicas

Example: replication factor 3, `min.insync.replicas=2`, `acks=all`. A write needs at least 2 in-sync replicas. If ISR drops below 2, Kafka rejects the write instead of accepting under-replicated data.

### Producer retries

`retries: 3` retries **retryable send failures** of a record. It does not mean reconnect attempts stop after three tries (connection recovery and metadata refresh are separate). `delivery.timeout.ms` limits how long a record can stay pending.

### Producer idempotence

`enable.idempotence=true` avoids duplicate log entries caused by the producer's own retries. It does **not** stop your app from deliberately sending the same order twice as two separate sends.

---

## 10. Spring Boot Producer

```text
HTTP Controller --> Service --> KafkaTemplate.send(...) --> Kafka topic
```

```yaml
server:
  port: 8081
spring:
  application:
    name: orders-producer
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      acks: all
      retries: 3
      properties:
        enable.idempotence: true
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
```

- `StringSerializer` turns string keys into bytes.
- `JsonSerializer` turns Java objects into JSON bytes.
- Kafka stores bytes, not Java objects, so producer and consumer formats must match.

Sending:
```java
kafkaTemplate.send("order-info", order);                       // no key
kafkaTemplate.send("order-info", order.getOrderId().toString(), order);  // with key
```
In production, check the async send result so failures are not silently ignored.

---

## 11. Spring Boot Consumer

```yaml
server:
  port: 8083
spring:
  application:
    name: inventory-consumer
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: inventory-group
      auto-offset-reset: earliest
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: com.inventoryapi.inventoryconsumer.model
        spring.json.value.default.type: com.inventoryapi.inventoryconsumer.model.Order
        spring.json.use.type.headers: false
```

Use the real package name of your `Order` class and trust only packages your app needs.

Listener:
```java
@KafkaListener(topics = "order-info", groupId = "${spring.kafka.consumer.group-id}")
public void consumeOrder(Order order) {
    inventoryService.processOrder(order);
}
```

Spring Kafka deserializes the JSON and calls the method. A second listener in the same group shares partitions, it does not automatically receive every record.

---

## 12. Offset Commits and Delivery Semantics

With `enable.auto.commit=false`, Spring Kafka's listener container commits offsets according to its acknowledgement mode, usually after the records of a poll are processed successfully.

Failure example:
1. Consumer reads order 203
2. Listener updates PostgreSQL
3. App crashes before the offset is committed
4. After restart, Kafka delivers order 203 again

That is why database **idempotency** matters: Kafka offset commits and PostgreSQL transactions are separate.

| Semantics | Meaning |
|---|---|
| At-most-once | never processed twice, but effects can be lost |
| At-least-once | processed one or more times, redelivery is possible |
| Exactly-once | supported inside Kafka transactions; external DB effects need extra design |

---

## 13. Retry, Backoff and Dead Letter Topic (DLT)

### Producer retry vs Consumer retry

| Producer retry | Consumer retry |
|---|---|
| happens while publishing a record | happens after the record is received and the listener fails |
| handles retryable send failures | handles application processing exceptions |
| set through producer config | usually set through Spring Kafka error handlers |
| does not retry consumer business logic | does not make the producer resend |

If a consumer is offline, Kafka just retains the record. The producer does not need to resend it.

### Retry flow

```text
order-info --> Inventory Consumer --> initial attempt
                                  --> retry 1
                                  --> retry 2
                                  --> retry 3
                                  --> recovery --> order-info-dlt
```

`new FixedBackOff(1000L, 3L)` means 1 second delay and 3 retries after the first attempt, so **4 attempts total** before recovery.

### Error handler with DLT

```java
@Bean
public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
    DeadLetterPublishingRecoverer recoverer =
        new DeadLetterPublishingRecoverer(
            kafkaTemplate,
            (record, exception) ->
                new TopicPartition(record.topic() + "-dlt", record.partition())
        );
    return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
}
```

Create the DLT with enough partitions, since the original partition number is preserved:
```cmd
kafka-topics.bat --bootstrap-server localhost:9092 --create --topic order-info-dlt --partitions 4 --replication-factor 3
```

Read it:
```cmd
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic order-info-dlt --from-beginning
```

Gotchas:
- The DLT producer needs a compatible serializer. Sending an `Order` object through a `StringSerializer` fails, so use a JSON value serializer and merge it into the existing `spring.kafka` YAML.
- **Transient** failures (brief DB outage) are worth retrying.
- **Permanent business outcomes** (insufficient stock, unknown product) should be a rejection, not retried forever.

---

## 14. PostgreSQL and Idempotency

Idempotency means processing the same order again has the same effect as processing it once.

```text
Without it:                          With it:
Initial stock 10                     Initial stock 10
Order 203 -> 9                       Order 203 -> 9
Duplicate 203 -> 8                   Duplicate 203 -> skipped
Duplicate 203 -> 7                   Duplicate 203 -> skipped
```

### Tables

```sql
CREATE TABLE processed_orders (
    order_id BIGINT PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    product_id BIGSERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL UNIQUE,
    available_quantity INTEGER NOT NULL CHECK (available_quantity >= 0)
);

INSERT INTO products (product_name, available_quantity)
VALUES ('Laptop', 10), ('Mouse', 50), ('Keyboard', 25);
```

### Entity and repository

```java
@Entity
@Table(name = "processed_orders")
public class ProcessedOrder {
    @Id
    @Column(name = "order_id")
    private Long orderId;

    @Column(name = "processed_at", nullable = false)
    private LocalDateTime processedAt;

    public ProcessedOrder() {}
    public ProcessedOrder(Long orderId) {
        this.orderId = orderId;
        this.processedAt = LocalDateTime.now();
    }
}

public interface ProcessedOrderRepository extends JpaRepository<ProcessedOrder, Long> {}
```

### Basic duplicate check

```java
if (processedOrderRepository.existsById(order.getOrderId())) {
    log.info("Duplicate order detected. Skipping: {}", order.getOrderId());
    return;
}
```

This is a good start, but two simultaneous deliveries can both pass the check. The **primary key is the real protection**, and production code must handle a duplicate-key conflict safely.

### Stock logic

1. Reject `quantity <= 0`
2. Check the product exists before reading its fields (avoids `NullPointerException`)
3. If available stock is less than requested, reject as insufficient stock
4. Otherwise reduce stock and record the order ID

Reduce stock and insert the processed order ID in the **same DB transaction**. For concurrency, use a row lock, optimistic locking, or an atomic conditional update.

Notes:
- `saveAndFlush()` sends pending SQL early so errors show up sooner. It does **not** commit the transaction.
- If PostgreSQL commits but the consumer crashes before the Kafka offset commit, the record is redelivered and the stored order ID lets you skip the duplicate effect.

Verify with SQL, since logs are evidence but queries confirm what is actually saved:
```sql
SELECT current_database(), current_schema();
SELECT * FROM processed_orders ORDER BY processed_at DESC;
SELECT * FROM products;
```

---

## 15. Troubleshooting

| Problem | What to check |
|---|---|
| `Node -1 disconnected`, connection failures | broker running, correct port, `listeners` and `advertised.listeners`, advertised addresses reachable from the client (every broker in a multi-broker cluster) |
| Broker stops with log directory error | each broker needs its own writable `log.dirs`; stop brokers before changing or deleting data (deleting can destroy records and metadata) |
| JSON deserialization errors | producer serializer, consumer deserializer, expected Java type, trusted packages, type headers, DLT serializer |
| Missing or incompatible Jackson classes | inspect Maven dependencies instead of adding random JARs |
| Hibernate SELECT but no INSERT | early return, missing product, insufficient stock, exception before save, transaction rollback, or querying a different database or schema |
| Consumer receives nothing | topic name, bootstrap server, group ID, committed offsets, partition assignment, `auto.offset.reset`, deserialization errors, retry logs |
| Windows "command not found" or "path not found" | current directory, Kafka extraction path, actual location of the properties files, switch drive with `d:` if needed |

Inspect dependencies:
```cmd
mvnw.cmd dependency:tree
mvnw.cmd dependency:tree | findstr /i "jackson kafka spring-kafka"
```

---

## 16. Commands Cheat Sheet

Run from `C:\kafka_2.13-3.9.0\bin\windows`:

| Task | Command |
|---|---|
| List topics | `kafka-topics.bat --bootstrap-server localhost:9092 --list` |
| Describe topic | `kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic order-info` |
| Create topic | `kafka-topics.bat --bootstrap-server localhost:9092 --create --topic order-info --partitions 4 --replication-factor 3` |
| Delete topic | `kafka-topics.bat --bootstrap-server localhost:9092 --delete --topic order-info` |
| List groups | `kafka-consumer-groups.bat --bootstrap-server localhost:9092 --list` |
| Describe group | `kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group inventory-group` |
| Delete group | `kafka-consumer-groups.bat --bootstrap-server localhost:9092 --delete --group inventory-group` |
| Console producer | `kafka-console-producer.bat --bootstrap-server localhost:9092 --topic order-info` |
| Console consumer | `kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic order-info` |
| Read from beginning | `kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic order-info --from-beginning` |
| Describe DLT | `kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic order-info-dlt` |
| Read DLT | `kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic order-info-dlt --from-beginning` |

Topic deletion, group deletion and offset resets change state, so use them carefully.

---

## 17. End-to-End Order Flow

1. Orders Producer serializes and publishes an order event to `order-info`
2. Kafka appends it to a partition and replicates it per the topic config
3. The Inventory Consumer's group owns that partition and polls the record
4. Spring Kafka deserializes the JSON into an `Order` and calls the listener
5. Inventory Service checks the order ID, product and stock
6. One PostgreSQL transaction updates stock and records the processed order ID
7. The listener succeeds and Spring Kafka commits the offset
8. If a duplicate arrives, the stored order ID makes the service skip it
9. If processing throws a retryable exception, the error handler retries, and once retries are exhausted the recoverer publishes to `order-info-dlt`

---

## 18. Key Distinctions to Remember

1. Topic = logical stream. Partition = ordered log inside the topic.
2. Offset = position inside a partition. Order ID = business identifier.
3. Broker = Kafka server. Consumer group = cooperating consumers.
4. Replication = data copies. Consumer groups = processing distribution.
5. Producer retry = retry publishing. Consumer retry = retry processing.
6. `auto.offset.reset` only applies when no valid committed offset exists.
7. Reading a record does not delete it.
8. Producer idempotence is not the same as business idempotency.
9. Retry tries processing again. DLT recovery sends the unrecovered record to another topic.
10. A PostgreSQL transaction and a Kafka offset commit are separate by default.
11. Logs are evidence, SQL queries confirm persisted state.
12. Insufficient stock is a business rejection, not necessarily a transient failure.

---

