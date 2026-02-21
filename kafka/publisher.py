from kafka import KafkaProducer

class KafkaPublisher:
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def send(self, topic, message: bytes):
        future = self.producer.send(topic, message)
        self.producer.flush()
        return future

# Connect to your Kafka broker
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],  # MUST use the external mapped port
)


# Send a message to the topic
future = producer.send('topic1', b'Hello, Kafka!')
producer.flush() 
print("Message sent!")