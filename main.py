import datetime
import os
import time
import psycopg
from functools import wraps
from abc import ABC, abstractmethod
from psycopg.rows import dict_row
from dotenv import load_dotenv
from state import State, JsonStorage
from elasticsearch import ElasticSearchLoader
import logging

logger = logging.getLogger(__name__)
formatter = logging.Formatter(fmt='[%(asctime)s: %(levelname)s] - %(message)s')



# Переделать базовый класс экстрактора для выведения туда общих запросов.


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        @wraps(func)
        def inner(instance, *args, **kwargs):
            time_to_sleep = 0
            counter = 0
            while True:
                try:
                    res = func(instance, *args, **kwargs)
                    return res
                except Exception as e:
                    time_to_sleep = start_sleep_time * (factor ** counter) 
                    if time_to_sleep > border_sleep_time:
                        time_to_sleep = border_sleep_time
                    logger.info(f'Пробую подключиться к базе данных повторно повторно. Жду {time_to_sleep}')
                    counter += 1
                    time.sleep(time_to_sleep)
                    instance.conn = instance.get_connection()          
        return inner
    return func_wrapper



class Database:

    def __init__(self, pg_data):
        self.pg_data = pg_data
        self.conn = self.get_connection(timeout=20)
        
            
    @backoff()
    def make_query(self, statement: str, **kwargs):
                cursor = self.conn.cursor()
                cursor.execute(statement)
                data = cursor.fetchall()
                return data

    # Получить таймаут из енва и сделать ожидание таймаута.
    def get_connection(self, timeout: int = 3):
        logger.info('Подключение к Postgres.')
        while timeout >= 0:
            try:
                conn = psycopg.connect(**self.pg_data, row_factory=dict_row)
                logger.info('подключено успешно.')
            except:
                timeout -= 1

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

    @abstractmethod
    def get_movies_ids_list(self):
        pass


class BaseExtractor(AbstractExtractor):

    def _get_movies_data(self, movies_ids: list):
        print(movies_ids)
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
    



class ExtractFilmWork(BaseExtractor):
    
    def extract_data(self):
        statement = '''SELECT id, modified FROM "content"."film_work"
                        WHERE modified > {modified_data}
                        ORDER BY modified
                        LIMIT 100;
        '''
        movies = self.database.make_query(statement)


    def get_movies_ids_list(self, modified_date: str):
        statement = f'''SELECT id, modified FROM "content"."film_work"
                        WHERE modified > '{modified_date}'
                        ORDER BY modified
                        LIMIT 100;
        '''
        movies = self.database.make_query(statement)
        return movies
        

    def get_movies_data(self, modified_date: str):
        movies = self.get_movies_ids_list(modified_date=modified_date)
        if not movies:
            return
        movies_ids = tuple([str(movie['id']) for movie in movies])
        all_data = self._get_movies_data(movies_ids)
        return all_data



class ExtractPerson(BaseExtractor):

    def __init__(self, *args, **kwargs):
        self.offset = 0
        super().__init__(*args, **kwargs)
    
    def extract_data(self):
            modified_persons = self._get_persons_from_db(50)
            modified_persons_id = tuple([person['id'] for person in modified_persons])
            return modified_persons_id


    
    def get_persons_id(self):
        modified_persons = self._get_persons_from_db(100)
        return modified_persons


    def get_movies_data(self, modified_persons_id: list, offset: int = 0):
        movies = self.get_movies_ids_list(ids_list=modified_persons_id, offset=offset)
        if not movies:
            self.offset = 0
            return
        movies_ids = tuple([str(movie['id']) for movie in movies])
        all_data = self._get_movies_data(movies_ids)
        return all_data

    # возможно принимать дату статуса, чтобы каждый раз не бегать в файл.
    def _get_persons_from_db(self, limit: int):
        current_state = self.state.get_storage('person')
        if not current_state:
            statement = f'SELECT id, modified FROM "content"."person" ORDER BY modified LIMIT {limit}'
        else:
            statement = f'SELECT id, modified FROM "content"."person" WHERE modified > \'{current_state}\' ORDER BY modified LIMIT {limit};'
        data = self.database.make_query(statement)
        return data
    

    def get_movies_ids_list(self, ids_list: list, offset: int):
        statement = f"""
                        SELECT DISTINCT fw.id, fw.modified
                        FROM content.film_work fw
                        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                        WHERE pfw.person_id IN {ids_list}
                        ORDER BY fw.modified
                        LIMIT 100
                        OFFSET {offset};
                        """

        data = self.database.make_query(statement)
        return data
        

class ExtractGenre(BaseExtractor):

    # возможно принимать дату статуса, чтобы каждый раз не бегать в файл.
    def _get_genres_from_db(self, limit: int):
        current_state = self.state.get_storage('person')
        if not current_state:
            statement = f'SELECT id, modified FROM "content"."genres" ORDER BY modified LIMIT {limit}'
        else:
            statement = f'SELECT id, modified FROM "content"."genres" WHERE modified > \'{current_state}\' ORDER BY modified LIMIT {limit};'
        data = self.database.make_query(statement)
        return data


    def get_movies_ids_list(self, ids_list: list, offset: int) -> dict:
        statement = f"""
                SELECT DISTINCT fw.id, fw.modified
                FROM content.film_work fw
                LEFT JOIN content.genre_film_work pfw ON pfw.film_work_id = fw.id 
                WHERE pfw.genre_id IN {ids_list}
                ORDER BY fw.modified
                LIMIT 100
                OFFSET {offset};
                """
        data = self.database.make_query(statement)
        return data

class Transform:
    
    def prepare_data(self, data: list):
        pass


class Transform:
    
    def prepare_data(self, data: list):
        result = {}
        for row in data:
            current_movie = result.setdefault(str(row['fw_id']), {})
            if not current_movie:
                current_movie['title'] = row['title']
                current_movie['description'] = row['description']
                current_movie['imdb_rating'] = row['rating']
                current_movie['genres'] = row['name']
                current_movie['title'] = row['title']
                current_movie['description'] = row['description']
                if row['role'] == 'director':
                    current_movie.setdefault('directors_names', []).append(row['full_name'])
                    current_movie.setdefault('directors', {}).update(id=str(row['id']), name=row['full_name'])
                elif row['role'] == 'actor':
                    current_movie.setdefault('actors_names', []).append(row['full_name'])
                    current_movie.setdefault('actors', {}).update(id=str(row['id']), name=row['full_name'])
                elif ['role'] == 'writer':
                    current_movie.setdefault('writers_names', []).append(row['full_name'])
                    current_movie.setdefault('writers', {}).update(id=str(row['id']), name=row['full_name'])
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
        # extractor_genre = ExtractGenre(db=db, state=state)
        self.extractor_filmwork = ExtractFilmWork(db=self.db,state=self.state)

        self.extractors = {
            'person': self.extractor_person,
            'genre': self.extractor_genre,
            'film_work': self.extractor_filmwork
        }

    
    def start(self):
        logger.info('Процесс запущен.')
        initial_state = True if not self.state.get_storage('movies') else False
        while True:
            try:
                self.process_film_work()
                if initial_state:
                    initial_state = False
                    continue
                not self.process_persons()
            except:
                self.db.close_connection()
            


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
            # ВСЕТАКИ ПРИВЕСТИ К ОДНОМУ МЕТОДУ
            rows = self.extractors[table_name].get_persons_id()
            if not rows:
                break
            logger.info(f'Из таблицы {table_name} получено {len(rows)} пользователей')
            rows_id = [row['id'] for row in rows]
            offset = 0
            while True:
                data = self.extractors[table_name].get_movies_data(rows_id)
                offset += 100
                if not data:
                    logger.info(f'Из базы {table_name} были получены все данные.')
                    break
                prepared_data = self.transformer.prepare_data(data)
                self.es_loader.batch_insert_data(prepared_data)
                logger.info(f'Успешно загружено {len(prepared_data)} документов')
            self.state.save_storage(table_name, str(rows[-1]['modified']))
            counter += len(rows)
            logger.info(f'Всего успешно обработано {counter} записей из таблицы {table_name}.')





    # def process_persons(self):
    #     logger.info('Началась обработка измененных пользователей')
    #     counter = 0
    #     while True:
    #         modified_persons = self.extractor_person.get_persons_id()
    #         if not modified_persons:
    #             break
    #         logger.info(f'Из базы получено {len(modified_persons)} пользователей')
    #         modified_persons_id = tuple(person['id'] for person in modified_persons)
    #         offset = 0
    #         while True:

    #             #добавлять время модификации, сохранять.
    #             # В стэйте сохранять person_date, genre_date, movies_date и так же временные даты для разовой выборки 
    #             data = self.extractor_person.get_movies_data(modified_persons_id)
    #             offset += 100
    #             if not data:
    #                 print('Вставка закончена')
    #                 break
    #             prepared_data = self.transformer.prepare_data(data)
    #             self.es_loader.batch_insert_data(prepared_data)
    #             logger.info(f'Успешно загружено {len(prepared_data)} документов')
    #         self.state.save_storage(str(modified_persons[-1]['modified']))
    #         counter += len(modified_persons)
    #         logger.info(f'Успешно обработано {counter} записей из таблицы person.')
            
                


if __name__ == '__main__':
    dsn = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': '123',
        'host': '172.18.0.3',
        'port': 5432,
    }

    etl = EtlProcess()
    etl.start()

            
        



