from config import Config
from logger.logger import Logger
from orchestrator import Orchestrator
from producer import Metadata_Producer
from handle_metadata import Handle_metadata


def main():
    logger = Logger().get_logger(name="metadata", index="metadata", level="INFO")
    logger.info("🚀 main started")
    try:
        config = Config(logger=logger)
        bootstrap_servers = config.bootstrap_servers
        topic = config.topic
        handle_metadata = Handle_metadata(logger=logger)
        producer = Metadata_Producer(
            logger=logger, bootstrap_servers=bootstrap_servers, topic=topic
        )
        orchestrator = Orchestrator(
            logger=logger, producer=producer, handle_metadata=handle_metadata
        )

        orchestrator.run("podcasts")
    except Exception as e:
        logger.error("failed running %s", e)


if __name__ == "__main__":
    main()
