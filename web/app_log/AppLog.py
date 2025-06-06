import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


class AppLog:

    def __init__( self, name: str = __name__, log_file: str = "app_log/app_log.csv", level=logging.DEBUG, max_bytes: int = 1_000_000, backup_count: int = 5 ):
        self.logger = logging.getLogger(name)

        if not self.logger.handlers:
            self.logger.setLevel(level)

            formatter = CSVFormatter()

            # Console handler (opcional)
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            console_handler.setLevel(level)

            # File handler com rotação
            file_handler = RotatingFileHandler(
                Path(log_file),
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            file_handler.setLevel(level)

            self.logger.addHandler(console_handler)
            self.logger.addHandler(file_handler)

    def get_logger(self):
        return self.logger



class CSVFormatter(logging.Formatter):
    def __init__(self):
        super().__init__()
        self.separator = ';'
        self.fields = ['asctime', 'levelname', 'name', 'message']  # ordem desejada

    def format(self, record):
        record.asctime = self.formatTime(record, self.datefmt)
        record.message = record.getMessage()

        # Coloca cada campo entre aspas duplas
        values = [f'"{str(getattr(record, field, ""))}"' for field in self.fields]
        return self.separator.join(values)
