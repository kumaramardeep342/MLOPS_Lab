import argparse
import json
import time
from collections import defaultdict
from kafka import KafkaConsumer


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--topic",
        required=True
    )
    parser.add_argument(
        "--group",
        default="group1"
    )
    parser.add_argument(
        "--bootstrap",
        default="localhost:9092"
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=5000
    )
    args = parser.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    print(f"\nListening → {args.topic}")
    print(f"Consumer Group → {args.group}\n")

    total = 0
    partition_count = defaultdict(int)
    start = time.time()
    try:
        for msg in consumer:
            total += 1
            partition_count[msg.partition] += 1
            if total % 100 == 0:
                print(
                    f"Consumed: {total}"
                )
            if total >= args.max_records:
                break
    except KeyboardInterrupt:
        print("\nStopped")
        
    elapsed = time.time() - start
    throughput = total / elapsed if elapsed else 0
    print("\n" + "="*25 + " CONSUMER METRICS REPORT " + "="*25)
    print(f"Records consumed : {total}")
    print(f"Elapsed seconds  : {elapsed:.2f}")
    print(f"Throughput       : {throughput:.2f} records/sec")
    print("\nPartition stats:")
    for p, count in sorted(partition_count.items()):
        print(
            f"Partition {p}: {count}"
        )
    consumer.close()


if __name__ == "__main__":
    main()