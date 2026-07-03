import argparse
import json
import time
from collections import defaultdict
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def ensure_kafka_topic(bootstrap_servers, topic_name):
    """Ensures the Kafka topic exists with 3 partitions and replication factor 1."""
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers, 
        client_id='admin_topic_initializer'
    )
    
    # Task 2: Configure with 3 partitions, replication factor = 1
    new_topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=1)
    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"[INFO] Created Kafka topic: '{topic_name}' (Partitions: 3, RF: 1)")
    except TopicAlreadyExistsError:
        print(f"[INFO] Kafka topic '{topic_name}' already exists. Proceeding to consumption.")
    finally:
        admin_client.close()

def main():
    parser = argparse.ArgumentParser(description="DA5402W MLOps Lab - Kafka Consumer")
    parser.add_argument("--topic", required=True, help="Kafka topic name, e.g., sensor_da25m502")
    parser.add_argument("--group", default="group1", help="Consumer group ID")
    parser.add_argument("--bootstrap", default="kafka:29092", help="Kafka broker address")
    parser.add_argument("--max-records", type=int, default=5000, help="Stop after consuming N records")
    args = parser.parse_args()

    # Step 1: Ensure the topic exists before spinning up the consumer
    ensure_kafka_topic(args.bootstrap, args.topic)

    # Step 2: Initialize Kafka Consumer
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    
    print(f"\n[STARTING] Listening to topic: {args.topic}")
    print(f"[STARTING] Active Consumer Group: {args.group}\n")

    total = 0
    partition_count = defaultdict(int)
    start_time = time.time()
    
    try:
        for msg in consumer:
            total += 1
            partition_count[msg.partition] += 1
            
            if total % 500 == 0:
                print(f"-> Consumed {total}/{args.max_records} records...")
                
            if total >= args.max_records:
                break
    except KeyboardInterrupt:
        print("\n[STOPPED] Execution interrupted by user.")
        
    elapsed = time.time() - start_time
    throughput = total / elapsed if elapsed > 0 else 0
    
    # Task 5: Print required evaluation metrics
    print("\n" + "="*25 + " METRICS REPORT " + "="*25)
    print(f"Total number of records consumed    : {total}")
    print(f"Total processing time (seconds)    : {elapsed:.2f}s")
    print(f"Consumer throughput (records/sec)  : {throughput:.2f}")
    print("-" * 66)
    print("Breakdown per Partition distribution:")
    for partition, count in sorted(partition_count.items()):
        print(f"  • Partition {partition}: {count} records received")
    print("="*66 + "\n")
    
    consumer.close()

if __name__ == "__main__":
    main()