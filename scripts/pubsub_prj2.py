from google.cloud import pubsub_v1

# Initialize a Subscriber client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path("prj2", "my-subscription")


# Callback function to process received messages
def callback(message):
    """
    This is a callback function to receive messages
    """
    print(f'Received message: {message.data.decode("utf-8")}')
    message.ack()


# Subscribe to the topic
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...")

try:
    streaming_pull_future.result()

except KeyboardInterrupt:
    streaming_pull_future.cancel()
