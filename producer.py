from kafka import KafkaProducer
import json
import time
import requests


def generate_api(params):
    response= requests.get(f'https://fakerapi.it/api/v2/books?_quantity=${params}')
    return response.json()


api=generate_api('10')

SERVER='localhost:9092'
TOPIC_NAME='fakerapi'

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
    print(books)
    producer.send(TOPIC_NAME,value=books)
    print(f"produced : {books}")
    time.sleep(1)

producer.flush()
print('All message are sent')