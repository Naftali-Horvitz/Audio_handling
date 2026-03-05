from confluent_kafka import Consumer
import logging, json
from typing import Callable


class Kafka_consumer:
    def __init__(
        self, logger: logging.Logger, bootstrap_servers: str, topic: str, group_id: str
    ):
        self.logger = logger
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id

    def start(self, callback: Callable[[dict], None]):
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest"
        }
        consumer = Consumer(config)
        consumer.subscribe([self.topic])
        self.logger.info(f"Kafka_consumer | 🟢 consumer is running and subscribe to {self.topic} topic")
        try:
            while True:
                event = consumer.poll()
                if event is None:
                    self.logger.warning("Kafka_consumer | There is no data to read")
                    continue
                if event.error():
                    self.logger.error("Kafka_consumer | ❌ Error: %s", event.error())
                    continue
                event = json.loads(event.value().decode("utf-8"))
                callback(event)
        except Exception as e:
            self.logger.exception("Kafka_consumer | %s", e)
        
        
        