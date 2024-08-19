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
                    db.get_connection()
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
            

    @backoff()
    def make_query(self, statement: str, **kwargs):
        cursor = self.conn.cursor()
        cursor.execute(statement)
        data = cursor.fetchall()
        return data

    def get_connection(self):
        self.conn =  psycopg.connect(**self.pg_data, row_factory=dict_row)

    def close_connection(self):
        self.connection.close()




class BaseExtractor(ABC):

    def __init__(self, db: Database):
        self.database = db

    @abstractmethod
    def extract_data(self):
        pass

class ExtractFilmWork(BaseExtractor):
    pass

class ExtractPerson(BaseExtractor):
    pass

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
        'host': '172.24.0.3',
        'port': 5432,
    }
    statement = 'select * from "content"."person";'
    d = Database(dsn)
    z = d.make_query(statement=statement)
    print(z)



