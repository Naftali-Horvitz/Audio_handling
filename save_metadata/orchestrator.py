import logging
from pathlib import Path
from consumer import Kafka_consumer
from handle_elasticsearch import Handle_elasticsearch
from handle_mongo import Handle_mongo


class Orchestrator:

    def __init__(
        self,
        logger: logging.Logger,
        es: Handle_elasticsearch,
        mongo: Handle_mongo,
        consumer: Kafka_consumer,
    ):
        self.logger = logger
        self.es = es
        self.mongo = mongo
        self.consumer = consumer

    def event_handle(self, metadata: dict):
        path = metadata.get("path")
        if path:
            p = Path(path)
            data_file = p.open(mode="rb")
            name = metadata.get("name")
            id = metadata.get("id")
            self.mongo.save_file(id=id, file=data_file, name=name)
            self.es.insert_data(metadata=metadata)
        else:
            self.logger.warning
    def run(self):
        self.consumer.start(self.event_handle)
