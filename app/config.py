import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass
class Postgres:
    host: str
    port: int
    db_name: str
    user: str
    password: str


@dataclass
class Elasticsearch:
    index_name: str
    host: str
    port: str
    file_schema: str


@dataclass
class State:
    file_name: str


@dataclass
class Extractor:
    limit: int


@dataclass
class Logger:
    file_name: str


@dataclass
class Config:
    postgres: Postgres
    elasticsearch: Elasticsearch
    state: State
    extractor: Extractor
    logger: Logger


def load_config(path: str = "./.env") -> Config:
    """Функция читает значения из .env или другого указанного файла
        и возвращает объект конфига с этими значениями.

    Returns:
        Сonfig: Объект конфига с прочитанными значениями.
    """
    load_dotenv()
    return Config(
        postgres=Postgres(
            host=os.environ.get("DB_HOST", "127.0.0.1"),
            port=os.environ.get("DB_PORT", 5432),
            db_name=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
        ),
        elasticsearch=Elasticsearch(
            index_name=os.environ.get("ES_INDEX"),
            host=os.environ.get("ES_HOST", "127.0.0.1"),
            port=os.environ.get("ES_PORT", 9200),
            file_schema=os.environ.get("ES_SCHEMA"),
        ),
        state=State(
            file_name=os.environ.get("STATE_FILE"),
        ),
        extractor=Extractor(
            limit=os.environ.get("LIMIT"),
        ),
        logger=Logger(file_name=os.environ.get("LOGGER_FILE", None)),
    )
