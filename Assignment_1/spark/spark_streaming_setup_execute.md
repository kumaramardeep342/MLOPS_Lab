### Details
- Roll No : DA25M502
- Name : Amardeep Kumar

### Software Requirement 
Spark : 3.5.6
Jdk 17
Apache Kakfka : 7.6.0
Docker

### Create Kafka topic
### Configure the topic 
docker exec -it docker-kafka-1 bash
kafka-topics --bootstrap-server localhost:9092 --create --topic sensor_da25m502 --partitions 3 --replication-factor 1
### Confirm the configuration
kafka-topics --bootstrap-server localhost:9092 --describe --topic sensor_da25m502

### First Terminal
python spark_streaming.py 
### Second Terminal
python producer.py --topic sensor_da25m502 --records 5000 --rate 50