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
        def inner(*args, **kwargs):
            db = args[0]
            t = 0
            counter = 0
            while True:
                try:
                    res = func(db, kwargs['statement'])
                    return res
                except Exception as e:
                    t = start_sleep_time * (factor ** counter) 
                    if t > border_sleep_time:
                        t = border_sleep_time
                    counter += 1
                    time.sleep(t)          
        return inner
    return func_wrapper



class Database:

    def __init__(self, pg_data):
        self.pg_data = pg_data
        self.conn = self.get_connection(self.pg_data)
            

    def make_query(self, statement: str, **kwargs):
        while True:
            try:
                cursor = self.conn.cursor()
                cursor.execute(statement)
                data = cursor.fetchall()
                return data
            except Exception as e:
                print(e)
                self.conn = self.get_connection(self.pg_data)

    def get_connection(self, pg_data, min_time=0.1, max_time=10):
        counter = 0
        while True:
            time.sleep(0.5)
            try:
                conn =  psycopg.connect(**pg_data, row_factory=dict_row)
                return conn
            except:
                delay = min_time * 2 ** counter
                time.sleep(delay)
                counter += 1

    def close_connection(self):
        self.connection.close()


class BaseExtractor(ABC):

    def __init__(self, db: Database):
        self.database = db
        self.shift = 0

    @abstractmethod
    def extract_data(self):
        pass

class ExtractFilmWork(BaseExtractor):
    pass

class ExtractPerson(BaseExtractor):
    def extract_data(self, limit: int = 100):
        statement = f'select * from "content"."person" limit {limit} offset {self.shift}'
        data = self.database.make_query(statement)
        self.shift += limit
        return data
class ExtractGenre(BaseExtractor):
    pass





            


# class PGextractor:


#     def __init__(self, dsn):
#         self.state = State(JsonStorage('hello.json'))

   

#     def update_persons(self, offset: int = None):
#         try:
#             start = int(self.state.get_state('shift'))
#         except:
#             start = 0

#         limit_num = 5
#         limit = f'\nLIMIT {limit_num};'
#         if offset:
#             limit = f'\nOFFSET {offset}' + limit
#         statement_person = """SELECT * FROM content.person
#                             WHERE modified > '1995.05.10'
#                             """ + limit
#         self.cursor.execute(statement_person)
#         data = self.cursor.fetchall()
#         modified_date = str(data[-1]['modified'])
#         print(modified_date)
#         self.state.save_storage('postgres', modified_date)
#         self.state.save_storage('shift', start + limit_num)
#         self.close_connection()

#         print(data)
          
    
#     def update_films(self):
#         pass

#     def update_genres(self):
#         pass

#     def connect(self):
#         pass


class EtlProcess:

    
    def start(self):
        pass
    

if __name__ == '__main__':
    dsn = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': '123',
        'host': '172.18.0.2',
        'port': 5432,
    }
    db = Database(pg_data=dsn)

    extractor = ExtractPerson(db=db)
    while True:
        

        z = extractor.extract_data(limit=20)
        



