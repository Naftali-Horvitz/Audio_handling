import logging, hashlib
from datetime import datetime
from pathlib import Path


class Handle_metadata:
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def generate_id(self, path) -> str:
        try:
            hash_md5 = hashlib.md5()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)

            return hash_md5.hexdigest()

        except FileNotFoundError:
            self.logger.exception("File not found while generating file id")
            raise
        except Exception:
            self.logger.exception("Failed generating file id")
            raise
    def create_metadata(self, audio_path: str):
        p = Path(audio_path)
        return {
            "id": self.generate_id(str(p.absolute())),
            "path": str(p.absolute()),
            "name": p.stem,
            "size": p.stat().st_size,
            "created_at": str(datetime.fromtimestamp(p.stat().st_ctime))
        }
