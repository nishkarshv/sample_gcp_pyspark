from google.cloud import pubsub_v1

# Initialize a Publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('prj1', 'my-topic')

# Publish a message
def publish_message(message):
    future = publisher.publish(topic_path, message.encode('utf-8'))
    print(f'Published message ID: {future.result()}')

if __name__ == "__main__":
    publish_message('Hello, Pub/Sub!')