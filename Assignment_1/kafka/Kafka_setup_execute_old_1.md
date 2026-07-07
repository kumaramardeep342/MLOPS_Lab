# 1. Create Kafka topic
```bash
docker exec -it docker-kafka-1 bash
```

# 2. Configure the topic 
```bash
kafka-topics \
--create \
--topic sensor_da25m502 \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1
```

```bash
kafka-topics \
--describe \
--topic sensor_da25m502 \
--bootstrap-server localhost:9092
```

**Output:**
```text
Partitions: 3
Replication factor: 1
```

# 3. Execute the provided producer and verify that records
```bash
python producer.py \
--topic sensor_da25m502 \
--records 5000 \
--rate 50
```

**Output:**
```text
Published 500/5000 records...
...
Done.
throughput = 49.7 records/sec
```

# 4. Consume the records from the topic and verify
```bash
docker exec -it docker-kafka-1 bash
```
```bash
kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic sensor_da25m502 \
--group group1 \
--from-beginning
```

# 5. Metrics
**total consumed**
```bash
kafka-consumer-groups \
--bootstrap-server localhost:9092 \
--describe \
--group group1
```

**Partition distribution**
```bash
kafka-run-class \
kafka.tools.GetOffsetShell \
--broker-list localhost:9092 \
--topic sensor_da25m502
```

**output**

| Metric Type | Performance Indicator | Value / Result |
| :--- | :--- | :--- |
| **Production** | Total number of records produced | *e.g., 5,000* |
| **Production** | Producer throughput (records/sec) | *e.g., 49.7 records/sec* |
| **Consumption** | Total number of records consumed | *e.g., 5,000* |
| **Consumption** | Consumer throughput (records/sec) | *e.g., 48.2 records/sec* |
| **Partitioning** | Records received from Partition 0 | *e.g., 1,665* |
| **Partitioning** | Records received from Partition 1 | *e.g., 1,670* |
| **Partitioning** | Records received from Partition 2 | *e.g., 1,665* |

# 6. Demonstrate consumer scaling
**One Consumer**
```bash
kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic sensor_da25m502 \
--group groupA
```
**Two Consumers**
```bash
kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic sensor_da25m502 \
--group groupB
```

# 7. Demonstrate independent consumer groups
```bash
--group analytics
```
```bash
--group monitoring
```