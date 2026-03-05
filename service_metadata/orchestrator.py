import logging
from pathlib import Path
from producer import Metadata_Producer
from handle_metadata import Handle_metadata


class Orchestrator:

    def __init__(
        self,
        logger: logging.Logger,
        producer: Metadata_Producer,
        handle_metadata: Handle_metadata,
    ):
        self.logger = logger
        self.producer = producer
        self.handle_metadata = handle_metadata

    def run(self, path_files: str):
        try:
            p = Path(path_files)
            self.logger.info("Orchestrator | %s", p)
        except Exception as e:
            self.logger.error("Orchestrator | failed path %s", e)

        for file_path in list(p.glob("*")):
            try:
                self.logger.debug("Orchestrator | %s", file_path)
                metadata = self.handle_metadata.create_metadata(file_path)
                self.logger.debug("Orchestrator | %s", metadata)
                self.producer.publish(metadata)
            except Exception as e:
                self.logger.error("Orchestrator | %s", e)
        self.producer.flush()
