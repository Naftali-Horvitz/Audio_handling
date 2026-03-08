import logging
from pathlib import Path
from consumer import Kafka_consumer
from elasticsearch_client import ElasticsearchClient
from transcription import Transcription



class Orchestrator:

    def __init__(
        self,
        logger: logging.Logger,
        es: ElasticsearchClient,
        consumer: Kafka_consumer,
        transcription: Transcription
    ):
        self.logger = logger
        self.es = es
        self.consumer = consumer
        self.transcription = transcription
        
        
    def event_handle(self, metadata: dict):
        path = metadata.get("path")
        if path:
            text_from_audio = self.transcription.get_text(path)
            new_metadata = metadata.copy()
            new_metadata["text"] = text_from_audio
            id = metadata.get("id")
            try:
                self.es.upsert(doc=new_metadata, id=id)
            except Exception as e:
                self.logger.exception("ElasticsearchClient | ❌ | %s", e)
        else:
            self.logger.warning("Orchestrator | not found path")
    
    def run(self):
        self.consumer.start(self.event_handle)
