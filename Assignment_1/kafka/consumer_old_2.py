"""
Kafka Consumer via PySpark - DA5402W Assignment 1
Reads data stream from Kafka and prints distribution and throughput performance metrics.
"""
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# BROKER = "kafka:29092"
BROKER = "localhost:9092"
TOPIC = "sensor_da25m502"

# Initialize Spark Session
spark = SparkSession.builder.appName("kafka-sensor-consumer").getOrCreate()

print(f"[INFO] Fetching records from topic '{TOPIC}'...")
start_time = time.time()

# Read from Kafka Topic from the beginning
raw_df = (spark.read.format("kafka")
          .option("kafka.bootstrap.servers", BROKER)
          .option("subscribe", TOPIC)
          .option("startingOffsets", "earliest")
          .load())

# Process and count records
total_records = raw_df.count()
elapsed = time.time() - start_time
throughput = total_records / elapsed if elapsed > 0 else 0

# Extract partition metrics directly using Spark's built-in column metadata
partition_counts = (raw_df.groupBy("partition")
                    .count()
                    .orderBy("partition")
                    .collect())

print("\n" + "="*25 + " CONSUMER METRICS REPORT " + "="*25)
print(f"Total number of records consumed    : {total_records}") 
print(f"Total processing time (seconds)    : {elapsed:.2f}s")
print(f"Consumer throughput (records/sec)  : {throughput:.2f}") 
print("-" * 66)
print("Breakdown per Partition distribution:") 
for row in partition_counts:
    print(f"  • Partition {row['partition']}: {row['count']} records received") 
print("="*66 + "\n")


# json format result
# consumer_metrics = {
#     "topic": args.topic,
#     "consumer_group": args.group,
#     "records_consumed": total,
#     "execution_time_sec": round(elapsed, 2),
#     "consumer_throughput_rps": round(throughput, 2),
#     "partition_distribution": dict(partition_count)
# }

# output_filename = f"reports/consumer_metrics_{args.group}.json"

# import os
# os.makedirs("reports", exist_ok=True)
# with open(output_filename, "w") as f:
#     json.dump(consumer_metrics, f, indent=4)
    
# print(f"[SUCCESS] Consumer metrics saved cleanly to {output_filename}")


spark.stop()