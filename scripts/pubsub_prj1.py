from google.cloud import pubsub_v1

# Initialize a Publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("arcane-world-420020", "my-topic")


# Publish a message
def publish_message(message):
    """
    This is a publisher function for Test
    """
    try:
        future = publisher.publish(topic_path, message.encode("utf-8"))
        print(f"Published message ID: {future.result()}")
    except Exception as e:
        raise e


if __name__ == "__main__":
    publish_message("Hello, Pub/Sub!")
