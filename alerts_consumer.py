from factories import new_kafka_consumer
from setup_topics import topic_temp, topic_alerts


def process_alerts():
    consumer = new_kafka_consumer(group_id='alerts_monitor')
    consumer.subscribe([topic_temp, topic_alerts])

    for message in consumer:
        alert = message.value
        print(f"Received alert from topic {message.topic}:")
        print(f"Sensor ID: {alert['sensor_id']}")
        print(f"Timestamp: {alert['timestamp']}")
        print(f"Message: {alert['message']}")
        print("---")


if __name__ == "__main__":
    process_alerts()
