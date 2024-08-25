import elasticsearch
from backoff import backoff
from config import load_config
from elasticsearch import helpers
from elasticsearch.exceptions import RequestError
from main_logger import MainLogger

logger = MainLogger().get_logger("elastic")


class ElasticSearchLoader:

    def __init__(self):
        self.config = load_config().elasticsearch
        self.index = self.config.index_name
        self.client = elasticsearch.Elasticsearch(
            f"http://{self.config.host}:{self.config.port}"
        )

    def _load_schema(self) -> str:
        """Функция читает схему из файла

        Args:
            path_file (str): Файл схемы.

        Returns:
            str: Схема, полученная из файла.
        """
        path_file = self.config.file_schema
        with open(path_file, "r") as file:
            schema = file.read()
        return schema

    @backoff()
    def create_index(self) -> None:
        """Функция создает индекс, если такой индекс уже существует, то
        пропускает создание.
        """
        schema = self._load_schema()
        try:
            logger.info(f"Создание индекса: {self.index}")
            self.client.indices.create(index=self.index, body=schema)
        except RequestError:
            logger.info(
                f"Пропускаю создание индекса. Индекс с названием уже существует.", self.index
            )

    def bulk_insert_data(self, data: dict) -> dict:
        """Функция вставляет массово вставляет данные

        Args:
            data (dict): Словарь с данными.

        Returns:
            dict: результат выполнения.
        """
        prepared_data = self.create_statement_bach_insert(data)
        return helpers.bulk(self.client, prepared_data, index=self.index)

    def create_statement_bach_insert(self, data: dict) -> list[dict]:
        """Функция формирует список для загрузки массовой загрузки.

        Args:
            data (dict): Словарь с данными

        Returns:
            list: Список с измененными значениями
        """
        prepared_data = []
        for _id, value in data.items():
            prepared_data.append({"_id": _id, "_index": self.index, "_source": value})
        return prepared_data
