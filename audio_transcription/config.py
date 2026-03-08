import os, logging


class Config:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        try:
            self.bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
            self.topic = os.getenv("CONSUMER_TOPIC", "METADATA")
            self.es_host = os.getenv("ES_HOST", "http://elasticsearch:9200")
            self.logger.debug(
                "Config | environments: %s, %s",
                self.bootstrap_servers,
                self.topic,
                self.es_host,
            )
        except:
            self.logger.exception("Config | error load env")
