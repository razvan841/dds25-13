from kafka import KafkaProducer

class KafkaPublisher:
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def send(self, topic, message: bytes):
        future = self.producer.send(topic, message)
        self.producer.flush()
        return future