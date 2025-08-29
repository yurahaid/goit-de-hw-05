from factories import new_kafka_consumer, new_kafka_producer
from setup_topics import topic_sensors, topic_temp, topic_alerts


def process_sensor_data():
    consumer = new_kafka_consumer(group_id='sensor_monitor')
    producer = new_kafka_producer()

    consumer.subscribe([topic_sensors])

    for message in consumer:
        data = message.value

        if data['temperature'] > 40:
            alert = {
                'sensor_id': data['sensor_id'],
                'timestamp': data['timestamp'],
                'temperature': data['temperature'],
                'message': 'Temperature exceeded 40°C threshold'
            }
            producer.send(topic_temp, value=alert)
            print(f"Sent alert to topic {topic_temp}: {alert}")

        if data['humidity'] > 80 or data['humidity'] < 20:
            alert = {
                'sensor_id': data['sensor_id'],
                'timestamp': data['timestamp'],
                'humidity': data['humidity'],
                'message': 'Humidity outside 20-80% range'
            }
            producer.send(topic_alerts, value=alert)
            print(f"Sent alert to topic {topic_alerts}: {alert}")


if __name__ == "__main__":
    process_sensor_data()
