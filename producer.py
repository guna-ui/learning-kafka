from kafka import KafkaProducer
import json
import time
import requests


def generate_api(params):
    response= requests.get(f'https://fakerapi.it/api/v2/books?_quantity={params}',timeout=10)
    return response.json()


api=generate_api('1000')

SERVER='localhost:9092'
TOPIC_NAME='faker-api'

producer=KafkaProducer(
    bootstrap_servers    = [SERVER],
    value_serializer    = lambda v : json.dumps(v).encode('utf-8')
)



for d in api['data']:
    books={
        'book_id'       : d['id'],
        'title'         : d['title'],
        'author'        : d['author'],
        'isbn'          : d['isbn'],
        'published'     : d['published']
    }
    # print(books)
    future = producer.send(TOPIC_NAME,value=books)

    record_metadata=future.get(timeout=10)
    print(
        f"✔ Produced to Topic: {record_metadata.topic}, "
        f"Partition: {record_metadata.partition}, "
        f"Offset: {record_metadata.offset}"
    )
    print("--------------------------------------------------")

    print(f"produced : {books}")
    time.sleep(1)

producer.flush()
print('All message are sent')