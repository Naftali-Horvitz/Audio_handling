from config import Config
from logger.logger import Logger
from orchestrator import Orchestrator
from consumer import Kafka_consumer
from handle_elasticsearch import Handle_elasticsearch
from handle_mongo import Handle_mongo


def main():
    logger = Logger().get_logger(name="metadata", index="metadata", level="INFO")
    logger.info("🚀 main started")
    try:
        config = Config(logger=logger)
        bootstrap_servers = config.bootstrap_servers
        topic = config.topic
        es_host = config.es_host
        mongo_uri = config.mongo_uri
        consumer = Kafka_consumer(
            logger=logger,
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            group_id="1",
        )
        es = Handle_elasticsearch(
            logger=logger, es_host=es_host, index="metadata_audio"
        )
        mongo = Handle_mongo(logger=logger, mongo_host=mongo_uri, db_name="audio_files")
        orchestrator = Orchestrator(
            logger=logger, es=es, mongo=mongo, consumer=consumer
        )
        orchestrator.run()

    except Exception as e:
        logger.error("failed running %s", e)


if __name__ == "__main__":
    main()
