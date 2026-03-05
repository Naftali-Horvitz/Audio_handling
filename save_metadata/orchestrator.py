import logging, hashlib
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

    def generate_id(self, path) -> str:
        try:
            hash_md5 = hashlib.md5()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)

            return hash_md5.hexdigest()

        except FileNotFoundError:
            self.logger.exception("File not found while generating file id")
            raise
        except Exception:
            self.logger.exception("Failed generating file id")
            raise

    def event_handle(self, metadata: dict):
        path = metadata.get("path")
        if path:
            p = Path(path)
            data_file = p.open(mode="rb")
            id = self.generate_id(path)
            name = metadata.get("name")
            self.mongo.save_file(id=id, file=data_file, name=name)
            self.es.insert_data(metadata=metadata)
        else:
            self.logger.warning
    def run(self):
        self.consumer.start(self.event_handle)
