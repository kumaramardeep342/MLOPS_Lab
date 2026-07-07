## Detail
- Roll No : DA25M502
- Name : Amardeep Kumar

## Software Requirement 
Apache Kakfka : 7.6.0
Docker

## 1. Create Kafka topic
## 2. Configure the topic 
docker exec -it docker-kafka-1 bash
kafka-topics --bootstrap-server localhost:9092 --create --topic sensor_da25m502 --partitions 3 --replication-factor 1
- confirm the configuration
kafka-topics --bootstrap-server localhost:9092 --describe --topic sensor_da25m502

## 3. Execute the provided producer and verify that records
python producer.py --topic sensor_da25m502 --records 5000 --rate 50
## 4. Consume the records from the topic and verify
python consumer.py --topic sensor_da25m502 --group group1 --bootstrap localhost:9092 --max-records 5000

## 5. Metrics

## 6. Demonstrate consumer scaling
## Testing Two Consumers
- step 1: Terminal 1 and start the consumer:
python consumer.py --topic sensor_da25m502 --group scaling_group_2 --bootstrap localhost:9092 --max-records 5000
- step 2 : Terminal 2 and start the producer:
python producer.py --topic sensor_da25m502 --bootstrap-servers localhost:9092 --records 5000 --rate 50


# Four Terminal : run this identical command in all four
python consumer.py --topic sensor_da25m502 --group scaling_group_4 --bootstrap localhost:9092 --max-records 5000
- Open a 5th terminal window and trigger your producer script again.
- Observation :  Because we have 3 partitions but 4 consumers, one of your consumers will sit completely idle. Kafka can only assign a partition to a single consumer in a group at any given time.

# 7. Demonstrate independent consumer groups
- step 1 : Terminal 1 (Group A):
python consumer.py --topic sensor_da25m502 --group analytics_pipeline --bootstrap localhost:9092 --max-records 5000
- step 2 : Terminal 2 Terminal 2 (Group B):
python consumer.py --topic sensor_da25m502 --group realtime_alerts --bootstrap localhost:9092 --max-records 5000
- step 3 : Terminal 3 -  run the producer script.
- obserbation : Both Terminal 1 and Terminal 2 will process all 5,000 records fully and concurrently.