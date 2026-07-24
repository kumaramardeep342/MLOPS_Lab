"""
Kafka Consumer - DA5402W Assignment 1, Part A.

Consumes sensor readings from a Kafka topic and reports:
  - total records consumed
  - records received per partition
  - consumer throughput (records/sec)

Supports running multiple consumers (in the same or different consumer
groups) as separate processes so that partition assignment / parallelism
and consumer-group isolation can be demonstrated.

Examples:
    # single consumer
    python kafka.py --topic sensor_21f1234567 --group-id grpA --num-consumers 1

    # four consumers in one group (max useful with 3 partitions = 3 active)
    python kafka.py --topic sensor_21f1234567 --group-id grpA --num-consumers 4

    # a second, independent consumer group reading the whole stream again
    python kafka.py --topic sensor_21f1234567 --group-id grpB --num-consumers 1
"""

import argparse
import json
import multiprocessing as mp
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer


def consume(topic, group_id, bootstrap_servers, consumer_id, idle_timeout_ms, result_queue, show_records=0):
    """Consume messages until no new message arrives for `idle_timeout_ms`."""
    print(f"[consumer-{consumer_id}] starting (group={group_id}, topic={topic})...")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=idle_timeout_ms,
        api_version=(3, 5, 0),
    )
    print(f"[consumer-{consumer_id}] subscribed, assigned partitions: {consumer.assignment()}")

    partition_counts = {}
    count = 0
    start_time = time.time()

    for msg in consumer:
        count += 1
        partition_counts[msg.partition] = partition_counts.get(msg.partition, 0) + 1
        if show_records and count <= show_records:
            print(f"[consumer-{consumer_id}] partition={msg.partition} "
                  f"offset={msg.offset} value={msg.value}")
        if count % 500 == 0:
            print(f"[consumer-{consumer_id}] consumed {count} records so far...")

    elapsed = time.time() - start_time
    print(f"[consumer-{consumer_id}] idle for {idle_timeout_ms}ms, stopping. "
          f"Total consumed: {count} in {elapsed:.2f}s")
    consumer.close()

    result_queue.put({
        "consumer_id": consumer_id,
        "group_id": group_id,
        "assigned_partitions": sorted(partition_counts.keys()),
        "records_consumed": count,
        "elapsed_seconds": elapsed,
        "throughput_rps": count / elapsed if elapsed > 0 else 0.0,
        "partition_counts": partition_counts,
    })


def run(topic, bootstrap_servers, group_id, num_consumers, idle_timeout_ms, show_records=0):
    result_queue = mp.Queue()
    processes = []

    for i in range(num_consumers):
        p = mp.Process(
            target=consume,
            args=(topic, group_id, bootstrap_servers, i, idle_timeout_ms, result_queue, show_records),
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    results = []
    while not result_queue.empty():
        results.append(result_queue.get())
    results.sort(key=lambda r: r["consumer_id"])

    total_records = sum(r["records_consumed"] for r in results)
    overall_elapsed = max((r["elapsed_seconds"] for r in results), default=0.0)

    partition_distribution = {}
    for r in results:
        for partition, c in r["partition_counts"].items():
            partition_distribution[partition] = partition_distribution.get(partition, 0) + c

    summary = {
        "topic": topic,
        "group_id": group_id,
        "num_consumers": num_consumers,
        "total_records_consumed": total_records,
        "elapsed_seconds": overall_elapsed,
        "consumer_throughput_rps": total_records / overall_elapsed if overall_elapsed > 0 else 0.0,
        "partition_distribution": partition_distribution,
        "per_consumer": results,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return summary


def main():
    #parser = argparse.ArgumentParser(description="Kafka sensor data consumer")
    topic = 'sensor_1'
    group_id = 'grpA'
    bootstrap_servers = 'kafka:29092'
    num_consumers = 3
    idle_timeout_ms = 10000
    show_records = 10
    
    #parser.add_argument("--topic", required=True, help="Kafka topic, e.g. sensor_21f1234567")
    #parser.add_argument("--bootstrap-servers", default="localhost:9092")
    #parser.add_argument("--group-id", required=True, help="Consumer group id")
    #parser.add_argument("--num-consumers", type=int, default=1,
    #                     help="Number of consumer processes to start in this group")
    #parser.add_argument("--idle-timeout-ms", type=int, default=10000,
     #                    help="Stop a consumer after this many ms with no new message")
    #parser.add_argument("--metrics-out", default="reports/consumer_metrics.json")
    #parser.add_argument("--show-records", type=int, default=0,
    #                     help="Print the full content of the first N records per consumer")
    #args = parser.parse_args()

    summary = run(
        topic=topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        num_consumers=num_consumers,
        idle_timeout_ms=idle_timeout_ms,
        show_records=show_records,
    )

    print(json.dumps(summary, indent=2))

    #try:
    #    with open(args.metrics_out, "w") as f:
    #        json.dump(summary, f, indent=2)
    #    print(f"Metrics written to {args.metrics_out}")
    #except OSError as e:
   #     print(f"Could not write metrics file: {e}")


if __name__ == "__main__":
    main()
