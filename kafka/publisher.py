from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='kafka-1:9092' 
)

producer.send('topic-1', b'Hello Kafka!')

producer.flush()

print("Message sent!")