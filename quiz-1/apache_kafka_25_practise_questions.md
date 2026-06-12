# Apache Kafka: 25 Practice Questions

## Section 1: Core Cluster Architecture & Internal Mechanics

### Q1 (MCQ)

In a production Apache Kafka cluster, what specific architectural role does the Controller broker fulfill that distinguishes it from other standard broker instances?

A. It manages client-side consumer offset state commits inside the `__consumer_offsets` system topic.

B. It is the sole broker responsible for encrypting and decrypting TLS network packets sent by producers.

C. It orchestrates administrative cluster-wide metadata changes, such as electing partition leaders and tracking node failures.

D. It acts as a stateless load balancer that evenly routes all incoming messages to worker nodes.

**Answer: C**

**Explanation:** The Controller is a single broker elected by the cluster that takes on the responsibility of managing partition states, tracking node membership joins/leaves, and executing partition leader elections when a broker fails.

### Q2 (MCQ)

When a topic is configured with multiple partitions, how does a Kafka broker physically organize and store message records on its local host operating system file system?

A. It merges all partitions globally into a single large, encrypted relational database table index.

B. It creates a dedicated directory for each partition containing a sequence of append-only log segments and their matching index files.

C. It holds data exclusively inside volatile JVM heap memory pools, flushing to flat CSV text logs only during graceful shutdowns.

D. It relies on internal client-side configurations to write files directly into an external Amazon S3 or Hadoop cloud bucket.

**Answer: B**

**Explanation:** Kafka writes data sequentially to disk for high throughput. Each partition maps directly to a physical directory on the broker's disk containing append-only `.log` data segments alongside `.index` and `.timeindex` pointer tracking tables.

### Q3 (MSQ)

Which of the following parameters are core components of a Kafka message record's physical structural anatomy as it travels through network segments? (Select ALL correct choices)

- [x] A. Key byte array
- [x] B. Value payload byte array
- [x] C. Timestamp metadata (LogAppendTime or CreateTime)
- [x] D. Optional User-defined Headers (Key-Value metadata pairs)

**Answer: A, B, C, D**

**Explanation:** A Kafka message record consists of a key, a value payload, a timestamp (which can be assigned by the client or upon appending to the broker log), and optional message headers introduced in modern Kafka versions for passing tracing/metadata fields.

### Q4 (MCQ)

If a Kafka topic is provisioned with a Replication Factor of 3, what does an In-Sync Replicas (ISR) status group explicitly represent to the cluster coordinator?

A. The set of all client consumers that have fully read and acknowledged the latest offset.

B. The specific subset of partition follower replicas that are actively alive, caught up, and matching the leader broker's log state within configured time limits.

C. The total number of backup nodes residing across separate geographic data centers.

D. The network connection threads handling data streaming allocations on the producer framework.

**Answer: B**

**Explanation:** The ISR list contains the leader replica and any follower replicas that are fully caught up with the leader's log. If a follower drops behind or stops polling within the `replica.lag.time.max.ms` window, it is dropped from the ISR list.

### Q5 (MSQ)

Identify all parameters or design features that contribute to Apache Kafka's ability to achieve millions of messages per second on standard hardware configurations: (Select ALL correct choices)

- [x] A. Utilizing Pagecache allocation heavily instead of running data operations inside volatile JVM heaps.
- [x] B. Employing the `sendfile()` system call to bypass user-space buffers and move data from disk directly to the network socket (Zero-Copy optimization).
- [x] C. Enforcing sequential append-only disk I/O log writes to maximize hardware performance.
- [x] D. Automatic client-side message batching and end-to-end data compression.

**Answer: A, B, C, D**

**Explanation:** Kafka's high-throughput architecture is achieved through sequential log writing (C), exploiting OS page cache memory (A), Zero-Copy processing to skip copying bytes into user memory spaces (B), and batching/compressing records before transmission (D).

## Section 2: Scenario-Based Failure Handling & Resiliency Engineering

### Q6 (MCQ – Scenario)

You are a Lead Platform Engineer configuring a critical financial transaction streaming topic. The business rules dictate that zero data loss can occur under any cluster outage scenario.

Which combination of Producer and Broker parameters must be explicitly set to ensure this standard of durability?

A. `acks=0` on the producer, with `min.insync.replicas=1` on the broker.

B. `acks=1` on the producer, with `retries=0` on the broker.

C. `acks=all` (or `-1`) on the producer, with `min.insync.replicas=2` on a topic with a replication factor of 3.

D. `compression.type=gzip` on the producer, with log cleanup compact policies.

**Answer: C**

**Explanation:** Setting `acks=all` means the producer will not receive a success acknowledgment until the message is written to the leader and all current ISR members. Combining this with `min.insync.replicas=2` ensures that at least two brokers have acknowledged the write before it is considered safe.

### Q7 (MCQ – Scenario)

An e-commerce company notices that during massive retail sale events, their real-time fraud detection consumer group begins falling drastically behind. The team wants to increase consumption throughput by scaling out their consumer instance fleet. The target Kafka topic currently has exactly 4 partitions. There are already 4 active consumer instances running inside the target Consumer Group.

If the engineering team spins up 2 additional consumer instances inside this exact same consumer group, what operational change will happen to the consumption pattern?

A. The new instances will take over 2 partitions, and the original 4 instances will dynamically share the remaining 2.

B. The 2 new instances will sit completely idle and will not be assigned any partitions, as Kafka limits partition assignment to a maximum of one consumer per group.

C. Kafka will automatically double the topic partition size to 8 to balance the resource fleet.

D. The cluster coordinator will crash instantly due to Consumer Group ID locking conflicts.

**Answer: B**

**Explanation:** Inside a single Consumer Group, Kafka balances consumption by assigning each partition to exactly one consumer instance. If the number of consumers exceeds the number of partitions ($6 > 4$), the extra consumers will remain idle as standby units.

### Q8 (MSQ – Scenario)

You are debugging an enterprise real-time feature engineering application that consumes telemetry data from a telemetry topic. You observe frequent Consumer Group Rebalances, which severely impacts processing performance and delays pipeline updates.

Which of the following execution issues or configuration values could be driving this behavior? (Select ALL correct choices)

- [x] A. The downstream business logic takes longer to process a batch of records than the duration set by `max.poll.interval.ms`.
- [ ] B. The network compression format was switched from Snappy to Zstd on the client machine.
- [x] C. A worker node hosting a consumer instance experiences severe JVM Garbage Collection pauses, failing to send heartbeats within the `session.timeout.ms` window.
- [ ] D. The producers are writing keys with an entirely random hash index profile.

**Answer: A, C**

**Explanation:** A rebalance is triggered whenever a consumer leaves or is presumed dead by the group coordinator. Failing to send heartbeats before `session.timeout.ms` expires (C) or taking too long to process records between polls beyond `max.poll.interval.ms` (A) causes the coordinator to evict the consumer and trigger a rebalance.

### Q9 (MCQ – Scenario)

A log analysis pipeline processes structured web events. The infrastructure team switches a topic's retention configuration from a time-based delete policy to a Log Compaction policy (`cleanup.policy=compact`).

What is the architectural impact of this modification on how data records are handled long-term?

A. The broker will automatically discard all historical records after exactly 7 calendar days.

B. The log segment cleaner thread will remove old records and retain only the latest single message value payload for each unique message key.

C. Every duplicate key is combined into an aggregated nested array using JSON Lines format structures.

D. Messages missing an explicit string schema are rejected by the partition leader.

**Answer: B**

**Explanation:** Log Compaction ensures that Kafka always retains at least the last known value for each message key within a topic partition log, discarding older duplicate keys during background segment cleanups.

## Section 3: Ecosystem Integrations & Stream Processing (Kafka Connect & Streams)

### Q10 (MCQ – Scenario)

You need to build a pipeline that continuously streams incremental data modifications from a production relational database (PostgreSQL) directly into Kafka topics without writing custom application logic.

Which specific ecosystem framework should you deploy to handle this integration natively?

A. Kafka Streams DSL

B. Kafka Connect with a Source Connector

C. Spark Structured Streaming Sink API

D. Airflow FileSensor Hooks

**Answer: B**

**Explanation:** Kafka Connect is the pluggable component for integrating Kafka with external systems. A Source Connector pulls data from external datastores (like relational databases via CDC) and writes it into Kafka topics.

### Q11 (MSQ – Scenario)

When developing real-time stream analytical systems using the Kafka Streams API, you must design exactly-once transactional flows. What core processing phases are bundled together atomically to achieve true Exactly-Once Semantics (EOS)? (Select ALL correct choices)

- [x] A. Consuming and recording input consumer progress offsets.
- [x] B. Executing internal stateful stream operations and window transformations.
- [x] C. Writing the finalized computed record outputs to destination target topics.
- [ ] D. Tracing geographical network proximity constraints across separate brokers.

**Answer: A, B, C**

**Explanation:** Exactly-Once Processing (EOS) in Kafka Streams relies on atomic transactions that wrap consumption offset tracking (A), internal state store changelog logging (B), and producer output writes (C) into a single transaction block that either completely succeeds or rolls back.

### Q12 (MCQ – Scenario)

An engineer is building a streaming dashboard using Kafka Streams to track rolling hourly totals of ride demands per city block zone. Because mobile tracking devices can experience connection dropouts, events occasionally arrive late and out of chronological order.

Which core processing semantic feature should the engineer configure to manage out-of-order data alignment properly?

A. Ingestion Time constraints using standard wall-clock ticks.

B. Event-Time semantics coupled with a defined Window grace period.

C. Processing-Time triggers running on the local broker thread.

D. Enforcing static `acks=0` pipelines to drop late rows.

**Answer: B**

**Explanation:** Event-Time represents the timestamp when the event occurred on the device. By using event-time analytics alongside window configurations that include a grace period, Kafka Streams can integrate late-arriving messages into their correct historical time windows.

## Section 4: Numerical, Partitioning, & Performance Calculations

### Q13 (MCQ – Numerical Partitioning)

A retail clickstream producer application publishes event messages to a target topic. The producer is using the default Kafka partitioner strategy. The input messages have a defined string key identifier field.

If the application sends 4 unique messages containing the key "User_101" to a topic containing 6 distinct partitions, how will Kafka distribute these messages across the cluster partitions?

A. The 4 messages will be distributed evenly across 4 separate partitions via round-robin routing.

B. All 4 messages will be routed to the exact same partition index number, because identical keys always hash to the same destination partition for a fixed partition count.

C. Every message will land exclusively on the active controller broker partition.

D. Kafka will reject 3 of the records as duplicate payload variants.

**Answer: B**

**Explanation:** Kafka's default partitioning strategy hashes message keys (`hash(key) % number_of_partitions`) to select a target partition. Because the key string "User_101" and the partition count (6) remain constant, all 4 messages are guaranteed to land on the same partition, preserving ordering.

### Q14 (MCQ – Cluster Offsets Math)

A high-throughput consumer application is actively pulling records from a topic partition. The cluster metrics report the following real-time numbers for a target partition log:

- Log End Offset (LEO): $1,250,000$
- Current Committed Offset: $1,180,000$
- High Watermark (HW): $1,240,000$

What is the precise Consumer Lag metrics value for this partition, and what is the maximum message offset currently available for safe consumption by standard consumers?

A. Lag = $70,000$; Max Available Offset = $1,250,000$

B. Lag = $60,000$; Max Available Offset = $1,240,000$

C. Lag = $10,000$; Max Available Offset = $1,180,000$

D. Lag = $70,000$; Max Available Offset = $1,240,000$

**Answer: B**

**Explanation:** Consumer Lag is the difference between the partition's High Watermark (the last record safely replicated to all ISR members) and the consumer group's last committed offset: $1,240,000 - 1,180,000 = 60,000$. Standard consumers cannot read past the High Watermark, so $1,240,000$ is the max safe offset.

### Q15 (MCQ – Operational Capacity Math)

A connected fleet of IoT devices transmits sensor payloads down to a Kafka ingestion cluster layer. The telemetry specifications indicate:

- Total active network devices = $50,000$
- Each individual device pushes exactly $1$ sensor payload string event every $2$ seconds
- The average payload record size is exactly $500 \text{ Bytes}$

What is the minimum aggregate network ingestion data throughput rate (in Megabytes per second) that your Kafka cluster brokers must be capable of processing continuously at peak capacity?

A. $50.0 \text{ MB/s}$

B. $12.5 \text{ MB/s}$

C. $25.0 \text{ MB/s}$

D. $5.0 \text{ MB/s}$

**Answer: B**

**Explanation:** 
1. Calculate messages per second: $50,000 \text{ devices} / 2 \text{ seconds} = 25,000 \text{ messages/second}$
2. Calculate raw bytes per second: $25,000 \text{ messages/s} \times 500 \text{ bytes/message} = 12,500,000 \text{ bytes/second}$
3. Convert to Megabytes per second: $12,500,000 \text{ B/s} / (10^6 \text{ B/MB}) = 12.5 \text{ MB/s}$

### Q16 (MCQ – Retention Calculus)

A system administrator creates a new Kafka topic using the configuration options listed below:

- `log.retention.bytes = 10737418240` (Exactly $10 \text{ GB}$ per partition log ceiling)
- `log.retention.hours = 24` (Exactly $1 \text{ Day}$ time threshold)

The topic contains exactly 4 partitions. A high-volume logging source pushes data into this topic at a sustained rate of $1 \text{ GB}$ per hour per partition.

Assuming the log cleaner engine runs its evaluation sweeps continually, after how many hours will old log segments begin getting deleted from the broker storage disk?

A. After 24 hours, because the time threshold takes precedence regardless of data volumes.

B. After 10 hours, because a partition's size will hit the $10 \text{ GB}$ ceiling first, triggering log cleanup.

C. After 40 hours, since the size threshold is calculated across all 4 partitions combined.

D. Data is never deleted because log compaction policies override size ceilings by default.

**Answer: B**

**Explanation:** Kafka enforces retention limits on a per-partition basis using an "OR" logic model—whichever threshold (size or time) is crossed first triggers deletion. Since data arrives at $1 \text{ GB/hour/partition}$, a partition hits its $10 \text{ GB}$ limit at 10 hours, which is well before the 24-hour time window.

## Section 5: Real-World Code & Configuration Snippets Analysis

### Q17 (MCQ – Configuration Code)

Review the following Python dictionary containing configuration properties for an Apache Kafka Producer client:

```python
producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'enable.idempotence': True,
    'acks': 'all',
    'max.in.flight.requests.per.connection': 5
}
```

What design guarantee is established by activating `enable.idempotence=True` in this producer layout?

A. The producer will duplicate every batch message to exactly two separate destination topics simultaneously.

B. It protects against message loss or out-of-order delivery caused by transient network retries without introducing duplicate records on the brokers.

C. It automatically switches the internal cluster catalog mode from ZooKeeper to KRaft.

D. It restricts the client to publishing records that contain numeric schemas only.

**Answer: B**

**Explanation:** An idempotent producer assigns a unique producer ID and sequence number to every message batch. If a network blip occurs and a retry is sent, the broker checks the sequence tracking metadata to reject duplicates while ensuring message ordering is maintained.

### Q18 (MCQ – Stream Code Output)

Observe this PySpark Structured Streaming code snippet designed to pull live streaming rows from an enterprise event hub:

```python
stream_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "taxi-rides")
    .option("startingOffsets", "latest")
    .load())

query = (stream_df.writeStream
    .format("console")
    .outputMode("append")
    .start())
```

If the consumer group goes offline, restarts 2 hours later, and executes this script, how will Spark compute data offsets?

A. It will automatically backfill and reprocess all messages produced during the 2 hours it was offline.

B. It will skip past the 2 hours of unread records and consume only new messages arriving after the current execution start time.

C. It will crash with a metadata offset validation exception.

D. It will pull records sequentially starting from offset position zero.

**Answer: B**

**Explanation:** Setting "startingOffsets" to "latest" instructs the engine to look only at the very end of the partition log when initializing a new stream, meaning it will skip historical records written while the application was offline.

### Q19 (MCQ – Code Execution Logic)

Analyze this real-time stream text processing function running inside an orchestration framework:

```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'iot-alerts',
    bootstrap_servers=['localhost:9092'],
    group_id='security-monitor',
    enable_auto_commit=True,
    auto_commit_interval_ms=5000
)

for message in consumer:
    payload = json.loads(message.value.decode('utf-8'))
    if payload.get("criticality") == "CRITICAL":
        print(f"ALARM triggered at offset: {message.offset}")
```

If `enable_auto_commit=True` is enabled, what operational risk does this application face if a critical JVM crash happens midway through processing?

A. The topic will delete all message partitions automatically.

B. The consumer might experience data loss or duplicate message processing because offsets are automatically committed on a background timer rather than when processing completes.

C. The script will halt because auto-commit forces the thread into a synchronous blocking write.

D. Kafka will assign a new randomized `group_id` value to the consumer block.

**Answer: B**

**Explanation:** With automatic background offset commits (`enable_auto_commit=True`), the client periodically commits the latest pulled offsets every 5 seconds. If the application crashes midway through processing records, those offsets might already be committed, resulting in uncommitted records being skipped (data loss) upon restart.

### Q20 (MCQ – Configuration Code)

An engineer evaluates a consumer group property configuration block:

```
heartbeat.interval.ms = 3000
session.timeout.ms = 45000
max.poll.records = 100
```

What is the architectural purpose of keeping the `heartbeat.interval.ms` value significantly smaller than the `session.timeout.ms` duration?

A. It forces the client to download small message batch arrays continuously.

B. It ensures the broker group coordinator receives multiple status sweeps to avoid falsely marking an alive consumer node as dead due to a minor network drop.

C. It allows the broker to execute automated log compact transformations in the background.

D. It resets the cluster metadata database index values back to zero automatically.

**Answer: B**

**Explanation:** The `heartbeat.interval.ms` defines how often the consumer background thread alerts the group coordinator broker that it is still alive. The coordinator waits up to `session.timeout.ms` before declaring a silent consumer dead and triggering a rebalance. Keeping heartbeats at $\le 1/3$ of the timeout prevents transient network issues from triggering accidental rebalances.

## Section 6: Multiple Select Questions (MSQs)

### Q21 (MSQ)

Which of the following characteristics accurately describe the technical differences between Kafka Topics and traditional Relational Database Management System (RDBMS) data tables? (Select ALL correct choices)

- [x] A. Kafka topics are fundamentally append-only sequential logs, whereas RDBMS tables allow arbitrary in-place data updates and row deletions.
- [ ] B. Kafka topics require an external Spark Session execution plan to view data profiles, while RDBMS engines do not.
- [x] C. Data inside a Kafka topic partition is consumed sequentially by reading from offset arrays, while RDBMS engines use indexes to look up arbitrary records directly.
- [x] D. Kafka decouples data producers from consumers, allowing multiple independent consumer groups to read from the same topic at their own pace.

**Answer: A, C, D**

**Explanation:** Kafka logs are strictly append-only (A) and rely on consumer group offset trackers to support independent, multi-client consumption (D). RDBMS engines use point updates and index lookups (C). Kafka does not require Spark to execute its core operations.

### Q22 (MSQ)

When configuring a Kafka Producer application, how does the setting chosen for the `acks` parameter directly affect performance, durability, and cluster operations? (Select ALL correct choices)

- [x] A. Setting `acks=0` provides the highest throughput and lowest latency but offers no durability guarantees if a broker crashes.
- [ ] B. Setting `acks=1` guarantees that every message is replicated to all follower nodes before the producer receives an acknowledgment.
- [x] C. Setting `acks=all` (or `-1`) offers the highest level of durability but can increase write latency because it waits for replication across the ISR list.
- [x] D. The choice of `acks` value modifies network transaction overhead on the broker network threads.

**Answer: A, C, D**

**Explanation:** `acks=0` means the producer never waits for a broker response (A). `acks=all` ensures the entire active ISR acknowledges the record before success is returned, increasing latency but maximizing durability (C, D). `acks=1` only waits for the individual partition leader node, not the follower replicas (B).

### Q23 (MSQ)

Which of the following internal events or conditions can cause a Consumer Group Rebalance to take place in a running cluster? (Select ALL correct choices)

- [x] A. A new consumer instance joins an existing consumer group by launching a script with an identical `group_id`.
- [x] B. An active consumer instance is cleanly shut down or experiences a hardware crash.
- [x] C. A cluster administrator alters a topic configuration to increase its partition count from 4 to 8.
- [ ] D. A producer shifts its data payload format from JSON string arrays to Avro binary streams.

**Answer: A, B, C**

**Explanation:** A rebalance occurs whenever the cluster coordinator needs to redistribute partition assignments. This can be triggered by a consumer joining (A), a consumer leaving or crashing (B), or a change to the underlying topic partitions being monitored (C). Changing producer serialization formats has no effect on group rebalances.

### Q24 (MSQ)

Identify all statements that accurately state the behavior and configuration rules of Kafka Partition Leaders and Follower Replicas: (Select ALL correct choices)

- [x] A. All client standard data write operations (producers) and read operations (consumers) are handled by the designated Partition Leader broker by default.
- [ ] B. Follower replicas participate in active Zookeeper voting protocols for every incoming data record batch.
- [x] C. Follower replicas continuously poll the partition leader to copy message segments sequentially to keep their logs synchronized.
- [x] D. If a partition leader broker goes offline, the cluster Controller selects one of the eligible in-sync follower replicas (ISR members) to become the new leader.

**Answer: A, C, D**

**Explanation:** Partition leaders handle all read/write client interactions by default (A). Follower replicas act as passive consumers that replicate data from the leader to provide fault-tolerance (C). If the leader fails, the Controller promotes an ISR member to leader (D). Follower nodes do not run consensus loops on a per-record basis.

### Q25 (MSQ)

When designing stateful real-time stream processors using Kafka, what specific architectural advantages does a Changelog Topic provide to a stateful application stream layout? (Select ALL correct choices)

- [x] A. It provides a durable backup of local state store modifications (like RocksDB entries) to support quick state recovery if a processing node fails.
- [ ] B. It completely eliminates the need to provision topic partitions on standard brokers.
- [x] C. It uses a log compaction cleanup policy to ensure it maintains the latest state updates without growing indefinitely.
- [ ] D. It forces Airflow executors to process micro-batch operations sequentially.

**Answer: A, C**

**Explanation:** Stateful operations (like windowed aggregations) store data locally in embedded databases (e.g., RocksDB) for low latency. To make this fault-tolerant, updates are written out to a background Changelog Topic configured with log compaction (C). If a stream node fails, a replacement instance can read this changelog to recover its state (A).
