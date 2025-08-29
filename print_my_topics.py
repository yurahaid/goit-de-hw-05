from admin_client import new_admin_client
from setup_topics import topics

admin_client = new_admin_client()


[print(topic) for topic in admin_client.list_topics() if topic in topics]

admin_client.close()