import psycopg
# from abc.


def get_pg_connection(self):
    pass

class PGextractor:


    def update_persons(self, offset: int = None):
        limit = '\nLIMIT 5;'
        if offset:
            limit = f'\nOFFSET {offset}' + limit
        statement_person = """SELECT * FROM content.person
                            WHERE modified > '1995.05.10'
                            """ + limit
        print(statement_person)
        
    
    def update_films(self):
        pass

    def update_genres(self):
        pass

    def connect(self):
        pass

d = PGextractor().update_persons(5)
# dsn = {
#     'dbname': 'postgres',
#     'user': 'postgres',
#     'password': '123',
#     'host': '172.18.0.3',
#     'port': 5432,
# }

# d = psycopg.connect(**dsn)

# with psycopg.connect(**dsn) as conn, conn.cursor() as cur:
#     statement = """SELECT * FROM content.person
#                     WHERE modified > '1995.05.10'
#                     LIMIT 1
#                     ;
#                     """
#     cur.execute(statement)
#     for _ in range(3):
#         d = cur.fetchall()
    
#         print(d)
#         print('---'* 30)