import logging

from kafka import KafkaConsumer
import json

# Create Kafka consumer, same default configuration frome the producer
from kafka_producer import topic_name


print("consumer11111111")
consumer = KafkaConsumer(
    "t1",
    bootstrap_servers=['localhost:9092'],)
    # value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Message loader from Json
for message in consumer:
    print(message)

