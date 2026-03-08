import os, logging


class Config:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        try:
            self.bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
            self.topic = os.getenv("PRODUCER_TOPIC", "METADATA")
            self.logger.info("Config | environments: %s, %s", self.bootstrap_servers, self.topic)
        except:
            self.logger.exception("Config | error load env")