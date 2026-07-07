"""
Kafka Producer via PySpark - DA5402W Assignment 1
Generates synthetic sensor readings and publishes them via the Spark-Kafka connector.
"""
import json
import random
import time
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

NUM_SENSORS = 10
STATUSES = ["active", "idle", "error", "maintenance"]
# BROKER = "kafka:29092"
BROKER = "localhost:9092"
TOPIC = "sensor_da25m502"
TOTAL_RECORDS = 5000

def build_record(sensor_id: int, ts: datetime, last_record: dict | None):
    """Exact logic from original instructor file to generate anomalies."""
    record = {
        "sensor_id": f"sensor_{sensor_id}",
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": round(random.uniform(15.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 90.0), 2),
        "status": random.choices(STATUSES, weights=[0.7, 0.15, 0.1, 0.05])[0],
    }
    roll = random.random()
    if roll < 0.03:
        record["temperature"] = None
    elif roll < 0.05:
        record["temperature"] = random.choice([-50.0, 150.0, -25.5, 120.0])
    elif roll < 0.07:
        record["timestamp"] = "NOT_A_TIMESTAMP"
    elif roll < 0.10:
        late_ts = ts - timedelta(minutes=random.randint(6, 15))
        record["timestamp"] = late_ts.strftime("%Y-%m-%d %H:%M:%S")
    elif roll < 0.12 and last_record is not None:
        return dict(last_record)
    return record

# Initialize Spark Session
spark = SparkSession.builder.appName("kafka-sensor-producer").getOrCreate()

print(f"[INFO] Generating {TOTAL_RECORDS} records locally...")
local_data = []
last_rec = None
base_ts = datetime.now(timezone.utc)

for i in range(TOTAL_RECORDS):
    # Increment timestamps slightly to simulate a continuous stream timeline
    current_ts = base_ts + timedelta(milliseconds=i * 20) 
    sid = random.randint(1, NUM_SENSORS)
    rec = build_record(sid, current_ts, last_rec)
    local_data.append(rec)
    last_rec = rec

# Convert Python dictionaries to JSON strings for Kafka Value
kafka_rows = [(rec["sensor_id"], json.dumps(rec)) for rec in local_data]

# Create Spark DataFrame matching Kafka source schema requirements (key, value)
start_time = time.time()
df = spark.createDataFrame(kafka_rows, ["key", "value"])

print(f"[INFO] Publishing batch data to Kafka topic '{TOPIC}'...")
(df.write.format("kafka")
 .option("kafka.bootstrap.servers", BROKER)
 .option("topic", TOPIC)
 .save())

elapsed = time.time() - start_time
throughput = TOTAL_RECORDS / elapsed if elapsed > 0 else 0

print("\n" + "="*25 + " PRODUCER METRICS " + "="*25)
print(f"Total number of records produced   : {TOTAL_RECORDS}") 
print(f"Total publishing time (seconds)    : {elapsed:.2f}s")
print(f"Producer throughput (records/sec)  : {throughput:.2f}") 
print("="*68 + "\n")

# json result
# metrics = {
#     "topic": args.topic,
#     "records_produced": args.records,
#     "execution_time_sec": round(elapsed, 2),
#     "producer_throughput_rps": round(throughput, 2)
# }

# # Save to JSON file
# import os
# if os.path.dirname(args.metrics-out):
#     os.makedirs(os.path.dirname(args.metrics-out), exist_ok=True)
    
# with open(args.metrics-out, "w") as f:
#     json.dump(metrics, f, indent=4)
# print(f"[SUCCESS] Producer metrics saved cleanly to {args.metrics-out}")

spark.stop()