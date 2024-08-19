import psycopg
from abc import ABC, abstractmethod
import os
from dotenv import load_dotenv
from state import State, JsonStorage, backoff
from psycopg.rows import dict_row
import datetime
from contextlib import contextmanager

def get_pg_connection(self):
    pass


class Database:

    def __init__(self, pg_data):
        self.db_data = pg_data
            
    @backoff()
    def make_query(self, **kwargs):
        print('heeey')


class BaseExtractor(ABC):

    def __init__(self, db: Database):
        pass

    @abstractmethod
    def extract_data(self):
        pass

class ExtractFilmWork(BaseExtractor):
    pass

class ExtractPerson(BaseExtractor):
    pass

class ExtractGenre(BaseExtractor):
    pass





# class Database:

#     def __init__(self, **kwargs):
#         self.db_data = kwargs
            


class PGextractor:


    def __init__(self, dsn):
        # self.connection = self.get_connection(dsn)
        # self.cursor = self.connection.cursor()
        self.state = State(JsonStorage('hello.json'))


    

    @contextmanager
    @backoff
    def get_connection(self, dsn):
        conn = psycopg.connect(**dsn, row_factory=dict_row)
        try:
            yield conn.cursor()
        except:
            conn.close()
    
    # def close_connection(self):
    #     self.connection.close()

    def update_persons(self, offset: int = None):
        try:
            start = int(self.state.get_state('shift'))
        except:
            start = 0

        limit_num = 5
        limit = f'\nLIMIT {limit_num};'
        if offset:
            limit = f'\nOFFSET {offset}' + limit
        statement_person = """SELECT * FROM content.person
                            WHERE modified > '1995.05.10'
                            """ + limit
        self.cursor.execute(statement_person)
        data = self.cursor.fetchall()
        modified_date = str(data[-1]['modified'])
        print(modified_date)
        self.state.save_storage('postgres', modified_date)
        self.state.save_storage('shift', start + limit_num)
        self.close_connection()

        print(data)
          
    
    def update_films(self):
        pass

    def update_genres(self):
        pass

    def connect(self):
        pass


class EtlProcess:

    
    def start(self):
        pass

dsn = {}

d = Database(dsn)
d.make_query(d.db_data)

# if __name__ == '__main__':
#     print(dsn)
#     d = PGextractor(dsn)
#     with d.get_connection(dsn) as c:
#         print('aaa')


