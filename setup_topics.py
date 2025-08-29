from kafka.admin import NewTopic

from factories import new_admin_client

my_name = "yurii_haid"
topic_sensors = f'{my_name}_building_sensors'
topic_temp = f'{my_name}_temperature_alerts'
topic_alerts = f'{my_name}_humidity_alerts'
topics = [topic_sensors, topic_temp, topic_alerts]
num_partitions = 2
replication_factor = 1


def create_topics():
    admin_client = new_admin_client()

    topics_to_create = []
    for topic_name in topics:
        topics_to_create.append(NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor))

    try:
        admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
    except Exception as e:
        print(f"An error occurred: {e}")

    admin_client.close()


if __name__ == "__main__":
    create_topics()
