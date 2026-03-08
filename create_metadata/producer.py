import logging, json
from confluent_kafka import Producer, KafkaException


class Metadata_Producer:
    def __init__(self, logger: logging.Logger, bootstrap_servers: dict, topic: str):
        self.logger = logger
        self.topic = topic
        try:
            self.producer = Producer({"bootstrap.servers": bootstrap_servers})
            self.logger.info("producer | Kafka producer initialized")
        except Exception:
            self.logger.error("producer | error creating producer")

    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"producer | delivery failed {err}")
        else:
            self.logger.debug(
                f"✅ producer | Delivered to {msg.topic()} "
                f"[{msg.partition()}] @ {msg.offset()}"
            )

    def publish(self, event: dict):
        try:
            self.producer.produce(
                topic=self.topic, value=json.dumps(event), callback=self.delivery_report
            )
            self.logger.info(f"✅ producer | send event")
            self.producer.poll(0)
        except KafkaException as e:
            self.logger.error(f"producer | Kafka exception {e}")

    def flush(self):
        try:
            self.producer.flush()
            self.logger.debug("producer | flushed all messages")

        except Exception:
            self.logger.error("producer | Kafka flush failed")
            raise
