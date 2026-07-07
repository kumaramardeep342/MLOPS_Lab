# 1. Create Kafka topic
# 2. Configure the topic 
python consumer.py --topic sensor_da25m502 --group group1 --max-records 5000

# 3. Execute the provided producer and verify that records
# 4. Consume the records from the topic and verify
python producer.py --topic sensor_da25m502 --records 5000 --rate 50

# 5. Metrics

# 6. Demonstrate consumer scaling
# Two terminal
python consumer.py --topic sensor_da25m502 --group scaling_group_2 --max-records 2500
# Four Terminal : run this identical command in all four
python consumer.py --topic sensor_da25m502 --group scaling_group_4 --max-records 1250
Open a 5th terminal window and trigger your producer script again.

# 7. Demonstrate independent consumer groups
step 1 : Terminal 1 (Group A):
python consumer.py --topic sensor_da25m502 --group analytics_pipeline
step 2 : Terminal 2 Terminal 2 (Group B):
python consumer.py --topic sensor_da25m502 --group realtime_alerts
step 3 : Terminal 3 -  run the producer script.
obserbation : Both Terminal 1 and Terminal 2 will process all 5,000 records fully and concurrently.