import psycopg
from psycopg.rows import dict_row
from abc import ABC, abstractmethod
import os
from dotenv import load_dotenv
from state import State, JsonStorage
from psycopg.rows import dict_row
import datetime
from contextlib import contextmanager
import time
from functools import wraps

def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        @wraps(func)
        def inner(instance, *args, **kwargs):
            t = 0
            counter = 0
            while True:
                try:
                    
                    res = func(instance, *args, **kwargs)
                    return res
                except Exception as e:
                    print(e)
                    t = start_sleep_time * (factor ** counter) 
                    if t > border_sleep_time:
                        t = border_sleep_time
                    counter += 1
                    time.sleep(t)
                    instance.conn = instance.get_connection()          
        return inner
    return func_wrapper



class Database:

    def __init__(self, pg_data):
        self.pg_data = pg_data
        self.conn = self.get_connection()
            
    @backoff()
    def make_query(self, statement: str, **kwargs):
                cursor = self.conn.cursor()
                cursor.execute(statement)
                data = cursor.fetchall()
                return data

    def get_connection(self):
        print('connect')
        return psycopg.connect(**self.pg_data, row_factory=dict_row)



    def close_connection(self):
        self.conn.close()


class BaseExtractor(ABC):

    def __init__(self, db: Database, state: State):
        self.database = db
        self.state = state

    @abstractmethod
    def extract_data(self):
        pass

class ExtractFilmWork(BaseExtractor):
    pass

class ExtractPerson(BaseExtractor):
    
    def extract_data(self):
            modified_persons = self._get_persons_from_db(50)
            if not modified_persons:
                return
            modified_persons_id = tuple([person['id'] for person in modified_persons])
            movies_state = self.state.get_storage('person_movies')
            movies = self._get_data_from_db('movies', ids_list=modified_persons_id)
            self.state.save_storage('person_movies', str(movies[-1].get('modified')))
            movies_ids = tuple([movie['id'] for movie in movies])
            all_data = self._get_data_from_db('all_tables', movies_ids)
            try:
                date = modified_persons[-1]['modified']
                self.state.save_storage('person', str(date))
            except:
                pass
            return all_data




    def _get_persons_from_db(self, limit: int):
        current_state = self.state.get_storage('person')
        if not current_state:
            statement = f'SELECT id, modified FROM "content"."person" ORDER BY modified LIMIT {limit}'
        else:
            statement = f'SELECT * FROM "content"."person" WHERE modified > \'{current_state}\' ORDER BY modified LIMIT {limit};'
        data = self.database.make_query(statement)
        return data
    

    def _get_data_from_db(self, table_name: str, ids_list: list):
        statement = self._get_statement(table_name=table_name, ids_list=ids_list)
        data = self.database.make_query(statement)
        return data
        

    def _get_statement(self, table_name: str, ids_list: list):
        statements = {
            'movies' : f"""
                        SELECT fw.id, fw.modified
                        FROM content.film_work fw
                        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                        WHERE pfw.person_id IN {ids_list}
                        ORDER BY fw.modified
                        LIMIT 100;
                        """,
            'all_tables': f"""
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
                        WHERE fw.id IN {ids_list}; 
                        """
            }
        return statements[table_name]







class ExtractGenre(BaseExtractor):
    pass


class Transform:
    
    def prepare_data(self, data: list):
        pass




class EtlProcess:

    
    def start(self):
        pass
    

if __name__ == '__main__':
    dsn = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': '123',
        'host': '172.18.0.3',
        'port': 5432,
    }
    state = State(storage=JsonStorage('hello.json'))
    db = Database(pg_data=dsn)

    extractor_person = ExtractPerson(db=db, state=state)
    # extractor_genre = ExtractGenre(db=db, state=state)
    # extract_filmwork = ExtractFilmWork(db=db,state=state)

    while True:

        try:
            data = extractor_person.extract_data()
            print(data[0])
            db.close_connection()
            break
            

        except KeyboardInterrupt:
            db.close_connection()

        # try:
        #     for extractor in [extractor_person, extractor_genre, extract_filmwork]:
        #         data_to_load = extractor.extract_data(limit=20)
        #         if not data_to_load:
        #             break
                

        # except KeyboardInterrupt:
        #     db.close_connection()
            
        



