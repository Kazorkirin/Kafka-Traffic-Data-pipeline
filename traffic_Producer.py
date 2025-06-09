import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, separators=(',', ':')).encode('utf-8')  # ensures valid JSON
)

locations = ['A1', 'A2', 'B1', 'B2']

while True:
    traffic_data = {
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'location': random.choice(locations),
        'vehicle_count': random.randint(10, 100),
        'average_speed': round(random.uniform(20.0, 100.0), 2)
    }

    print(f"Sending to Kafka: {traffic_data}")
    producer.send('traffic_topic', traffic_data)
    time.sleep(1)
