import gridfs, logging
from pymongo import MongoClient


class Handle_mongo:
    def __init__(self, logger: logging.Logger, mongo_host: str, db_name: str):
        self.logger = logger
        self.client = MongoClient(mongo_host)
        self.db = self.client[db_name]
        self.fs = gridfs.GridFS(self.db)

    def save_file(self, id, name, file):
        try:
            data = file.read()
            self.fs.put(data, filename=name, file_id=id)
            self.logger.info("file saved to mongo")
        except Exception as e:
            self.logger.exception("Handle_mongo | %s", e)
