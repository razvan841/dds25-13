from abc import ABC, abstractmethod




# ---------- Event Handler Interface ----------
class EventHandler(ABC):
    @abstractmethod
    def handle_message(self, message: str):
        """Business logic"""
        pass


# ---------- Kafka Publisher Interface ----------
class KafkaPublisher(ABC):
    @abstractmethod
    def publish(self, topic: str, message: str):
        """Publish message to topic in Kafka"""
        pass



# ---------- Kafka Subscriber Interface ----------
class KafkaSubscriber(ABC):
    @abstractmethod
    def listen(self, handler: EventHandler):
        """Poll events from topic"""
        for message in self.consumer:
            handler.handle_message(message)
        pass


