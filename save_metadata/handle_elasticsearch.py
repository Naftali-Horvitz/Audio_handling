from elasticsearch import Elasticsearch
import logging


class Handle_elasticsearch:
    def __init__(self, logger: logging.Logger, es_host: str, index: str):
        self.logger = logger
        self.index = index
        self.es = Elasticsearch(es_host)
        self.create_index()

    def create_index(self):
        mapping = {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "path": {"type": "keyword"},
                    "name": {"type": "keyword"},
                    "size": {"type": "integer"},
                    "created_at": {"type": "text"},
                }
            }
        }
        if not self.es.indices.exists(index=self.index):
            self.es.indices.create(index=self.index, body=mapping)

    def insert_data(self, metadata: dict):
        try:
            res = self.es.index(index=self.index, body=metadata)
            self.logger.info(
                "Handle_elasticsearch | save index to elasticsearch %s", res["result"]
            )
        except Exception as e:
            self.logger.exception("Handle_elasticsearch | error save metadata %s", e)
