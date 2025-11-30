from kafka import KafkaConsumer
import json
import time

SERVER='localhost:9092'
TOPIC_NAME='faker-api'


consumer=KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers  = [SERVER],
    auto_offset_reset  = 'earliest',
    enable_auto_commit = True,
    value_deserializer = lambda v : json.loads(v.decode('utf-8')),
    group_id           = "book-group"
)


print("Listening for messages...")

for message in consumer:
        print(message.value)
        print("----- MESSAGE RECEIVED -----")
        print(f"Partition      : {message.partition}")
        print(f"Offset         : {message.offset}")
        print(f"Topic          : {message.topic}")
        print(f"Book Processed : {message}")
        print("----------------------------\n")
        time.sleep(0.5)
