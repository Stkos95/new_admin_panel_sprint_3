import time
from abc import ABC, abstractmethod

import psycopg
from backoff import backoff
from elasticsearch_class import ElasticSearchLoader
from main_logger import MainLogger
from psycopg.rows import dict_row
from state import JsonStorage, State

from config import load_config

logger = MainLogger().get_logger("main")
config = load_config()


class Database:

    def __init__(self, pg_data: dict):
        self.pg_data = pg_data
        self.conn = self.get_connection()

    def make_query(self, statement: str, timeout: int = 1) -> list | None:
        """Функция выполняет запрос к базе данных.

        Args:
            statement (str): Запрос для выполнения.
            timeout (int, optional): Время, в течение которого осуществляется повторный запрос . Defaults to 1.

        Returns:
            dict: Полученные данные.
        """
        while timeout > 0:
            try:
                cursor = self.conn.cursor()
                cursor.execute(statement)
                data = cursor.fetchall()
                return data
            except:
                time.sleep(timeout)
                timeout -= 1
                self.conn = self.get_connection()
        return

    @backoff()
    def get_connection(self):
        """Функция выполняет подключение к базе данных.

        Returns:
            connection: Объект подключения к базе данных.
        """
        logger.info("Подключение к Postgres.")
        conn = psycopg.connect(**self.pg_data, row_factory=dict_row)
        logger.info("подключено успешно.")
        return conn

    def close_connection(self):
        self.conn.close()


class AbstractExtractor(ABC):

    def __init__(self, db: Database, state: State):
        self.database = db
        self.state = state

    @abstractmethod
    def extract_data(self):
        """Функция получает данные из определенной таблицы."""
        pass


class BaseExtractor(AbstractExtractor):

    def _get_movies_data(self, movies_ids: tuple[str]) -> dict:
        """Функция получает финальные данные по указанным фильмам.

        Args:
            movies_ids (list): Список айдишников фильмов.

        Returns:
            dict: Полученные данные.
        """
        movies_ids = "( " + ", ".join([f"'{mov}'" for mov in movies_ids]) + " )"
        statement = f"""
            SELECT
                fw.id as fw_id, 
                fw.title, 
                fw.description, 
                fw.rating, 
                fw.type, 
                fw.created, 
                fw.modified, 
                pfw.role, 
                p.id, 
                p.full_name,
                g.name,
                g.id as g_id
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id IN {movies_ids}; 
            """
        return self.database.make_query(statement=statement)

    def get_movies_data(self, movies: list[dict]) -> dict:
        """Функция принимает список фильмов, получает айдишники этих
            фильмов и получает финальные значения по ним.

        Args:
            movies (list): Список фильмов, полученных из базы данных.

        Returns:
            dict: Финальные данные для вставки.
        """
        movies_ids = tuple([str(movie["id"]) for movie in movies])
        all_data = self._get_movies_data(movies_ids)
        return all_data

    def _get_data_from_db(self, table_name: str) -> list:
        """Функция получает измененные данные в зависимости от таблицы.

        Args:
            table_name (str): Название таблицы.

        Returns:
            list: Список с полученными значениями измененных строк.
        """
        current_state = self.state.get_storage(table_name)
        if not current_state:
            statement = f'SELECT id, modified FROM "content"."{table_name[:-1]}" ORDER BY modified LIMIT {config.extractor.limit}'
        else:
            statement = f'SELECT id, modified FROM "content"."{table_name[:-1]}" WHERE modified > \'{current_state}\' ORDER BY modified LIMIT {config.extractor.limit};'
        data = self.database.make_query(statement)
        return data


class ExtractFilmWork(BaseExtractor):

    def extract_data(self):
        current_state = self.state.get_storage("film_work") or "1111-11-11"
        statement = f"""SELECT id, modified FROM "content"."film_work"
                        WHERE modified > '{current_state}'
                        ORDER BY modified
                        LIMIT {config.extractor.limit};
        """
        return self.database.make_query(statement)


class ExtractPerson(BaseExtractor):

    def __init__(self, *args, **kwargs):
        self.offset = 0
        super().__init__(*args, **kwargs)

    def extract_data(self):
        modified_persons = self._get_data_from_db("persons")
        return modified_persons

    def get_movies_list(self, modified_items_ids: tuple, modified_date: str):
        """Функция получает список фильмов по измененным строкам в таблице person.

        Args:
            modified_items_ids (str): Айдишники измененных строк.
            modified_date (str): Дата изменения последнего вставленного элемента

        Returns:
            list: Список с полученными значениями фильмов.
        """
        statement = f"""
                        SELECT DISTINCT fw.id, fw.modified
                        FROM content.film_work fw
                        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                        WHERE pfw.person_id IN {modified_items_ids} AND fw.modified > '{modified_date}'
                        ORDER BY fw.modified
                        LIMIT {config.extractor.limit};
                        """
        return self.database.make_query(statement)


class ExtractGenre(BaseExtractor):

    def get_movies_list(self, modified_items_ids: tuple, modified_date: str):
        """Функция получает список фильмов по измененным строкам в таблице genre.

        Args:
            modified_items_ids (str): Айдишники измененных строк.
            modified_date (str): Дата изменения последнего вставленного элемента

        Returns:
            list: Список с полученными значениями фильмов.
        """
        statement = f"""
                SELECT DISTINCT fw.id, fw.modified
                FROM content.film_work fw
                LEFT JOIN content.genre_film_work pfw ON pfw.film_work_id = fw.id 
                WHERE pfw.genre_id IN {modified_items_ids} AND fw.modified > '{modified_date}'
                ORDER BY fw.modified
                LIMIT {config.extractor.limit};
                """
        return self.database.make_query(statement)

    def extract_data(self):
        return self._get_data_from_db("genres")


class Transform:
    def __init__(self):
        self.transformers = {
            'movies': self.prepare_data_movies,
            'genres': self.prepare_data_genres,
            'persons': self.prepare_data_persons
        }

    def prepare_data_movies(self, data: list) -> dict:
        """Функция переводит финальные данные по фильмам в вид для вставки в ES

        Args:
            data (list): Данные фильмов.

        Returns:
            dict: Словарь с измененными данными.
        """
        result = {}
        for row in data:
            current_movie = result.setdefault(str(row["fw_id"]), {})
            if not current_movie:
                current_movie["id"] = str(row["fw_id"])
                current_movie["title"] = str(row["title"])
                current_movie["description"] = str(row["description"])
                current_movie["imdb_rating"] = row["rating"] or 0
                current_movie["title"] = str(row["title"])
                current_movie["description"] = str(row["description"])
                for role in ("directors", "actors", "writers"):
                    current_movie[role] = []
                    current_movie[f"{role}_names"] = []
            genres = current_movie.setdefault("genres", [])
            if row["name"] not in genres:
                genres.append({'id': str(row["g_id"]), 'name': str(row["name"])})
            role = row["role"]
            if role == "director":
                directors_name = current_movie.setdefault("directors_names", [])
                if str(row["full_name"]) not in directors_name:
                    directors_name.append(str(row["full_name"]))
                    current_movie.setdefault("directors", []).append(
                        dict(id=str(row["id"]), name=str(row["full_name"]))
                    )
            elif role == "actor":
                actors_name = current_movie.setdefault("actors_names", [])
                if str(row["full_name"]) not in actors_name:
                    actors_name.append(str(row["full_name"]))
                    current_movie.setdefault("actors", []).append(
                        dict(id=str(row["id"]), name=str(row["full_name"]))
                    )
            elif role == "writer":
                writers_name = current_movie.setdefault("writers_names", [])
                if str(row["full_name"]) not in writers_name:
                    writers_name.append(str(row["full_name"]))
                    current_movie.setdefault("writers", []).append(
                        dict(id=str(row["id"]), name=str(row["full_name"]))
                    )
        return result
    
    def prepare_data_persons(self, data: list):
        result = {}
        for row in data:
            current_person = result.setdefault(str(row['id']), {})
            if not current_person:
                current_person['id'] = str(row['id'])
                current_person['full_name'] = str(row['full_name'])
            movies = current_person.setdefault('movies_names', [])
            if row['title'] not in movies:
                current_person.setdefault('movies', []).append({'id': str(row['fw_id']), 'title': row['title'], 'role': row['role']})
                movies.append(row['title'])
        return result
    

    def prepare_data_genres(self, data: list):
        result = {}
        for row in data:
            current_genre = result.setdefault(str(row['g_id']), {})
            if not current_genre:
                current_genre['id'] = str(row['g_id'])
                current_genre['name'] = row['name']
            movies = current_genre.setdefault('movies_names', [])
            if row['title'] not in movies:
                current_genre.setdefault('movies', []).append({'id': str(row['fw_id']), 'title': row['title']})
                movies.append(row['title'])
        return result



class MovieIndex:
    def __init__(self, db, state, es_loader: ElasticSearchLoader):
        self.index = 'movies'

        es_loader.create_index(self.index)



class PersonIndex:
    """SELECT id, full_name from "content"."persons"
        WHERE modified > date


    """

class EtlProcess:
    def __init__(self):
        self.state = State(storage=JsonStorage())
        self.db = Database(pg_data=dsn)
        self.transformer = Transform()
        self.es_loader = ElasticSearchLoader()
        # self.es_loader_persons = ElasticSearchLoader('persons')
        # self.es_loader_persons = ElasticSearchLoader('genres')

        self.extractor_person = ExtractPerson(db=self.db, state=self.state)
        self.extractor_genre = ExtractGenre(db=self.db, state=self.state)
        self.extractor_filmwork = ExtractFilmWork(db=self.db, state=self.state)
        self.es_loader.create_index('movies')
        self.es_loader.create_index('genres')
        self.es_loader.create_index('persons')
    
        self.extractors = {
            "movies": self.extractor_filmwork,
            "persons": self.extractor_person,
            "genres": self.extractor_genre,
        }

    def start(self):
        """Функция запускает процесс."""
        logger.info("Процесс запущен.")
        while True:
            for extractor in self.extractors:
                try:
                    self.universal_process(extractor)
                except KeyboardInterrupt:
                    self.db.close_connection()
                    exit()
            logger.info("Итерация завершена!")


    def universal_process(self, table_name: str):
        """Функция принимает название таблицы и производит получение/трансформацию/вставку
            данных.

        Args:
            table_name (str): Название таблицы.
        """
        logger.info(f"Началась обработка таблицы: %s", table_name)
        counter = 0
        while True:
            is_go = True
            self.state.save_storage("tmp_date", "1111-11-11")
            rows = self.extractors[table_name].extract_data()
            if not rows:
                break
            logger.info(f"Из таблицы %s получено %s записей", table_name, len(rows))
            rows_id = tuple(row["id"] for row in rows)
            while is_go:
                tmp_date = self.state.get_storage("tmp_date")
                if table_name != "movies":
                    movies_list = self.extractors[table_name].get_movies_list(
                        rows_id, tmp_date
                    )
                else:
                    movies_list = rows
                    is_go = False
                if not movies_list:
                    break
                data = self.extractors[table_name].get_movies_data(movies_list)
                prepared_data = self.transformer.transformers[table_name](data)
                z = self.es_loader.bulk_insert_data(prepared_data, table_name)
                if table_name != 'movies':
                    prepared_data = self.transformer.transformers['movies'](data)
                    self.es_loader.bulk_insert_data(prepared_data, 'movies')
                logger.info(f"Успешно загружено %s документов", len(movies_list))
                self.state.save_storage("tmp_date", str(data[-1]["modified"]))
            self.state.save_storage(table_name, str(rows[-1]["modified"]))
            counter += len(rows)
            logger.info(
                f"Всего успешно обработано %s записей из таблицы %s.", counter, table_name
            )


if __name__ == "__main__":
    dsn = {
        "dbname": config.postgres.db_name,
        "user": config.postgres.user,
        "password": config.postgres.password,
        "host": config.postgres.host,
        "port": config.postgres.port,
    }
    etl = EtlProcess()
    etl.start()
