from kafka import KafkaConsumer

class KafkaSubscriber:
    def __init__(self, bootstrap_servers, group_id, topics):
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id
        )
        self.consumer.subscribe(topics)

    def start(self, handler):
        for message in self.consumer:
            handler.handle_message(message)


# Connect to your Kafka broker
consumer = KafkaConsumer(
    'topic-1',                  # topic name
    bootstrap_servers='kafka-1:9092',  # broker address
    auto_offset_reset='earliest',  # read from the beginning
    group_id='test-group'          # consumer group
)

print("Listening for messages...")
for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")