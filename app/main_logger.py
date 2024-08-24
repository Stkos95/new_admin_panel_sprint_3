import logging
from logging import Logger

from config import load_config


class MainLogger:

    def get_logger(self, name: str) -> Logger:
        """Функция создает и возвращает объект логгера.

        Args:
            file (str, optional): Путь к файлу логов, если не указан, то выводится в sttdout. Defaults to None.

        Returns:
            Logger: Объект логгера.
        """
        file_name = load_config().logger.file_name
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(fmt="[%(asctime)s: %(levelname)s] - %(message)s")
        if file_name:
            handler = logging.FileHandler(file_name)
        else:
            handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
