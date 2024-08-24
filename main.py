import datetime
import os
import time
import psycopg
from functools import wraps
from abc import ABC, abstractmethod
from psycopg.rows import dict_row
from dotenv import load_dotenv
from state import State, JsonStorage
from elasticsearch1 import ElasticSearchLoader
import logging
from requests.exceptions import ConnectionError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(fmt='[%(asctime)s: %(levelname)s] - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)



# Переделать базовый класс экстрактора для выведения туда общих запросов.


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        @wraps(func)
        def inner(instance, *args, **kwargs):
            time_to_sleep = 0
            counter = 0
            while True:
                try:
                    # (psycopg.OperationalError, ConnectionError) 
                    return func(instance, *args, **kwargs)
                except Exception as e:
                    print(e)
                    time_to_sleep = start_sleep_time * (factor ** counter) 
                    if time_to_sleep > border_sleep_time:
                        time_to_sleep = border_sleep_time
                    logger.info(f'Пробую подключиться к базе данных повторно повторно. Жду {time_to_sleep}')
                    counter += 1
                    time.sleep(time_to_sleep)
                except KeyboardInterrupt:
                    exit() 
        return inner
    return func_wrapper



class Database:

    def __init__(self, pg_data):
        self.pg_data = pg_data
        self.conn = self.get_connection(timeout=20)
        
            
    
    def make_query(self, statement: str, timeout: int = 1, **kwargs):
                while timeout > 0:
                    try:
                        cursor = self.conn.cursor()
                        cursor.execute(statement)
                        data = cursor.fetchall()
                        return data
                    except Exception as e:
                        print(e)
                        print('here')
                        time.sleep(timeout)
                        timeout -= 1
                        self.conn = self.get_connection()
                return 
                    


    @backoff()
    def get_connection(self, timeout: int = 3):
        logger.info('Подключение к Postgres.')
        conn = psycopg.connect(**self.pg_data, row_factory=dict_row)
        logger.info('подключено успешно.')
        return conn


    def close_connection(self):
        self.conn.close()


class AbstractExtractor(ABC):

    def __init__(self, db: Database, state: State):
        self.database = db
        self.state = state

    @abstractmethod
    def extract_data(self):
        pass

    # @abstractmethod
    # def get_movies_ids_list(self):
    #     pass


class BaseExtractor(AbstractExtractor):

    def _get_movies_data(self, movies_ids: list):
        movies_ids = '( ' + ', '.join([f"'{mov}'" for mov in movies_ids ]) + ' )'
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
                g.name
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id IN {movies_ids}; 
            """
        return self.database.make_query(statement=statement)
    

    # def get_movies_data(self, modified_items_ids: list, modified_date: str, statement: str):
    #     movies = self.get_movies_list(statement=statement)
    #     if not movies:
    #         return
    #     movies_ids = tuple([str(movie['id']) for movie in movies])
    #     all_data = self._get_movies_data(movies_ids)
    #     return all_data
    
    def get_movies_data(self, movies: list) -> dict:
        movies_ids = tuple([str(movie['id']) for movie in movies])
        all_data = self._get_movies_data(movies_ids)
        return all_data

    



class ExtractFilmWork(BaseExtractor):
    
    def extract_data(self):
        current_state = self.state.get_storage('film_work') or '1111-11-11'
        statement = f'''SELECT id, modified FROM "content"."film_work"
                        WHERE modified > '{current_state}'
                        ORDER BY modified
                        LIMIT 100;
        '''
        return self.database.make_query(statement)

    # def get_movies_ids_list(self, statement: str):

    #     # statement = f'''SELECT id, modified FROM "content"."film_work"
    #     #                 WHERE modified > '{modified_date}'
    #     #                 ORDER BY modified
    #     #                 LIMIT 100;
    #     # '''

    #     movies = self.database.make_query(statement)
    #     return movies
        

class ExtractPerson(BaseExtractor):

    def __init__(self, *args, **kwargs):
        self.offset = 0
        super().__init__(*args, **kwargs)
    
    def extract_data(self):
        modified_persons = self._get_persons_from_db(500)
        return modified_persons

    
    def get_movies_list(self, modified_items_ids: tuple, modified_date: str):
        statement = f"""
                        SELECT DISTINCT fw.id, fw.modified
                        FROM content.film_work fw
                        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                        WHERE pfw.person_id IN {modified_items_ids} AND fw.modified > '{modified_date}'
                        ORDER BY fw.modified
                        LIMIT 100;
                        """
        return self.database.make_query(statement)

    def get_total(self):
        return self.database.make_query('select count(*) from "content"."person";')



    # возможно принимать дату статуса, чтобы каждый раз не бегать в файл.
    def _get_persons_from_db(self, limit: int):
        current_state = self.state.get_storage('person')
        if not current_state:
            statement = f'SELECT id, modified FROM "content"."person" ORDER BY modified LIMIT {limit}'
        else:
            statement = f'SELECT id, modified FROM "content"."person" WHERE modified > \'{current_state}\' ORDER BY modified LIMIT {limit};'
        data = self.database.make_query(statement)
        return data
    


class ExtractGenre(BaseExtractor):

    # возможно принимать дату статуса, чтобы каждый раз не бегать в файл.
    def _get_genres_from_db(self, limit: int, modified_date: str = None):
        if not modified_date:
            current_state = self.state.get_storage('genre')
        if not current_state:
            statement = f'SELECT id, modified FROM "content"."genre" ORDER BY modified LIMIT {limit}'
        else:
            statement = f'SELECT id, modified FROM "content"."genre" WHERE modified > \'{current_state}\' ORDER BY modified LIMIT {limit};'
        data = self.database.make_query(statement)
        return data


    def get_movies_list(self, modified_items_ids: tuple, modified_date: str):
        statement = f"""
                SELECT DISTINCT fw.id, fw.modified
                FROM content.film_work fw
                LEFT JOIN content.genre_film_work pfw ON pfw.film_work_id = fw.id 
                WHERE pfw.genre_id IN {modified_items_ids} AND fw.modified > '{modified_date}'
                ORDER BY fw.modified
                LIMIT 100;
                """
        return self.database.make_query(statement)


    def extract_data(self, modified_date: str = None):
        return self._get_genres_from_db(100, modified_date)

class Transform:
    
    def prepare_data(self, data: list):
        result = {}
        # result = []
        for row in data:
            current_movie = result.setdefault(str(row['fw_id']), {})
            # print(current_movie)
            if not current_movie:
                current_movie['id'] = str(row['fw_id'])
                current_movie['title'] = str(row['title'])
                current_movie['description'] = str(row['description'])
                current_movie['imdb_rating'] = row['rating'] or 0
                # current_movie['genres'] = str(row['name'])

                current_movie['title'] = str(row['title'])
                current_movie['description'] = str(row['description'])
                for role in ('directors', 'actors', 'writers'):
                    current_movie[role] = []
                    current_movie[f'{role}_names'] = []
            current_movie.setdefault('genres', []).append(str(row['name']))
            role = row['role']
            if role == 'director':
                directors_name = current_movie.setdefault('directors_names', [])
                if str(row['full_name']) not in directors_name:
                    directors_name.append(str(row['full_name']))
                    current_movie.setdefault('directors', []).append(dict(id=str(row['id']), name=str(row['full_name'])))
            elif role == 'actor':
                actors_name = current_movie.setdefault('actors_names', [])
                if str(row['full_name']) not in actors_name:
                    actors_name.append(str(row['full_name']))
                    current_movie.setdefault('actors', []).append(dict(id=str(row['id']), name=str(row['full_name'])))
            elif role == 'writer':
                writers_name = current_movie.setdefault('writers_names', [])
                if str(row['full_name']) not in writers_name:
                    writers_name.append(str(row['full_name']))
                    current_movie.setdefault('writers', []).append(dict(id=str(row['id']), name=str(row['full_name'])))
        result = list(result.values())
        return result


class EtlProcess:
    def __init__(self):
        self.state = State(storage=JsonStorage('hello.json'))
        self.db = Database(pg_data=dsn)
        self.extractor_person = ExtractPerson(db=self.db, state=self.state)
        self.transformer = Transform()
        # возможно вынести название индекса в конфиг и путь к схеме.
        self.es_loader = ElasticSearchLoader('movies')
        self.es_loader.create_index(schema='schema.json')
        self.extractor_genre = ExtractGenre(db=self.db, state=self.state)
        self.extractor_filmwork = ExtractFilmWork(db=self.db,state=self.state)

        self.extractors = {
            'film_work': self.extractor_filmwork,
            'person': self.extractor_person,
            'genre': self.extractor_genre,
        }

    
    def start(self):
        logger.info('Процесс запущен.')
        while True:
            for extractor in self.extractors:
                try:
                    self.universal_process(extractor)
                except KeyboardInterrupt as e:
                    print(e)
                    self.db.close_connection()
                    exit()
            logger.info('Итерация завершена!')
            


    def process_film_work(self):
        while True:
            modified_date = self.state.get_storage('movies') or datetime.datetime(year=1111, month=11, day=11)
            data = self.extractor_filmwork.get_movies_data(modified_date=modified_date)
            if not data:
                break
            self.state.save_storage('movies', str(data[-1]['modified']))

            

    def universal_process(self, table_name: str):
        logger.info(f'Началась обработка таблицы: {table_name}')
        counter = 0
        while True:
            time.sleep(1)
            is_go = True
            self.state.save_storage('tmp_date', '111-11-11')
            rows = self.extractors[table_name].extract_data()
            if not rows:
                break
            logger.info(f'Из таблицы {table_name} получено {len(rows)} пользователей')
            rows_id = tuple(row['id'] for row in rows)
            while is_go:
                tmp_date = self.state.get_storage('tmp_date')
                if table_name != 'film_work':
                    movies_list = self.extractors[table_name].get_movies_list(rows_id, tmp_date)
                    if not movies_list:
                        break
                    data = self.extractors[table_name].get_movies_data(movies_list)
                    
                    prepared_data = self.transformer.prepare_data(data)
                    
                    for row in prepared_data:
                        record = self.es_loader.search_field(row['id'])['hits']

                        if record['total']['value'] != 0:
                            
                            record_id = record['hits'][0]['_id']
                            z = self.es_loader.client.update(index=self.es_loader.index, id=record_id, doc=row)
                            print(z)
                else:
                    movies_list = rows
                    is_go = False
                    data = self.extractors[table_name].get_movies_data(movies_list)
                    prepared_data = self.transformer.prepare_data(data)
                    d = self.es_loader.batch_insert_data(prepared_data)
                if not movies_list:
                    break

                logger.info(f'Успешно загружено {len(movies_list)} документов')
                self.state.save_storage('tmp_date', str(data[-1]['modified']))
            self.state.save_storage(table_name, str(rows[-1]['modified']))
            counter += len(rows)
            logger.info(f'Всего успешно обработано {counter} из  записей из таблицы {table_name}.')


if __name__ == '__main__':
    dsn = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': '123',
        'host': '127.0.0.1',
        'port': 5432,
    }

    etl = EtlProcess()
    etl.start()

            
        



