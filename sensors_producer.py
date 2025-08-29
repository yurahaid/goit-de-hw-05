from factories import new_kafka_producer
import random
import time
from setup_topics import topic_sensors
from datetime import datetime


def run_sensors():
    producer = new_kafka_producer()
    sensor_id = random.randint(1, 1000)

    while True:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        temperature = round(random.uniform(25, 45), 2)
        humidity = round(random.uniform(15, 85), 2)

        data = {
            'sensor_id': sensor_id,
            'timestamp': timestamp,
            'temperature': temperature,
            'humidity': humidity
        }

        producer.send(topic_sensors, value=data)
        print(f"Sent data to topic {topic_sensors}: {data}")
        time.sleep(5)


if __name__ == "__main__":
    run_sensors()
