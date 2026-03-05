import logging
from datetime import datetime
from pathlib import Path


class Handle_metadata:
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def create_metadata(self, audio_path: str):
        p = Path(audio_path)
        return {
            "path": str(p.absolute()),
            "name": p.stem,
            "size": p.stat().st_size,
            "created_at": str(datetime.fromtimestamp(p.stat().st_ctime))
        }
