from config import Config
from logger.logger import Logger
from orchestrator import Orchestrator
from consumer import Kafka_consumer
from elasticsearch_client import ElasticsearchClient
from transcription import Transcription


def main():
    logger = Logger().get_logger(name="metadata", index="metadata", level="INFO")
    logger.info("🚀 main started")
    try:
        config = Config(logger=logger)
        bootstrap_servers = config.bootstrap_servers
        topic = config.topic
        es_host = config.es_host
        es = ElasticsearchClient(logger=logger, es_host=es_host, index="metadata_audio")
        consumer = Kafka_consumer(
            logger=logger,
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            group_id="2",
        )
        transcription = Transcription(logger=logger)
        orchestrator = Orchestrator(logger=logger, consumer=consumer, es=es, transcription=transcription)

        orchestrator.run()
    except Exception as e:
        logger.error("failed running %s", e)


if __name__ == "__main__":
    main()
