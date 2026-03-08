from elasticsearch import Elasticsearch
import logging


class ElasticsearchClient:
    def __init__(self, logger: logging.Logger, es_host: str, index: str):
        self.logger = logger
        self.index = index
        try:
            self.es = Elasticsearch(es_host)
        except Exception as e:
            self.logger.exception("ElasticsearchClient | ❌ | %s", e)


    def upsert(self, doc: dict, id: str):
        try: 
            res = self.es.update(
                index=self.index,
                id=id,
                doc=doc,
                doc_as_upsert=True,
                refresh="wait_for",
            )
            self.logger.info("ElasticsearchClient | %s", res["result"])
            doc_search = self.es.get(index=self.index, id=id)
            self.logger.debug("ElasticsearchClient | doc after the updated: %s", doc_search)
        except Exception as e:
            self.logger.exception("ElasticsearchClient | ❌ | %s", e)
            
