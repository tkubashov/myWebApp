import os
import sqlite3
from dataclasses import asdict

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor, execute_batch

from sqlite_to_postgres.movies_dataclasses_copy import sqlrow_to_dataclass



class SQLiteLoader:

    @staticmethod
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def __init__(self, connector):
        self.conn = connector
        self.conn.row_factory = self.dict_factory
        self.cur = self.conn.cursor()


    def load_from_sqlite(self, table, columns):
        """Универсальный метод загрузки данных из таблиц БД SQLite."""
        try:
            columns_in_table = str(columns)[1:-1].replace("'", "")
            self.cur.execute(f"""select {columns_in_table} from {table}""")
        except (Exception, psycopg2.Error) as err:
            print('Error: ', err, table)
        while True:
            rows = self.cur.fetchmany(1000)
            if not rows:
                break
            else:
                movies = [
                    sqlrow_to_dataclass(table, row)
                    for row in rows
                ]
                PostgresSaver(pg_conn).save_all_data(movies, table, columns_in_table)


class PostgresSaver:
    def __init__(self, connector):
        self.conn = connector
        self.cur = self.conn.cursor()

    def save_all_data(self, data, table, columns_in_table):
        values = tuple(f'%({column})s' for column in columns_in_table.split(', '))
        values_to_postgre = str(values).replace("'", "")
        try:
            execute_batch(
                self.cur,
                f"""INSERT INTO content.{table} ({columns_in_table})
                    VALUES {values_to_postgre}""",
                [asdict(film_work) for film_work in data],
                page_size=1000,
            )
        except (Exception, psycopg2.Error) as err:
            print('Error: ', err, table)

    def truncate_table(self, table):
        """Метод предварительной очистки данных из таблиц БД PostgreSQL."""
        self.cur.execute(f"""TRUNCATE content.{table} CASCADE""")



def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres."""
    list_tables = [{'table': 'film_work', 'columns': ('title', 'description', 'creation_date', 'certificate',
                                                      'file_path', 'type', 'rating', 'created_at', 'updated_at',
                                                      'id')},
                   {'table': 'person', 'columns': ('full_name', 'birth_date', 'created_at', 'updated_at', 'id')},
                   {'table': 'genre', 'columns': ('name', 'description', 'created_at', 'updated_at', 'id')},
                   {'table': 'person_film_work', 'columns': ('film_work_id', 'person_id', 'role', 'created_at', 'id')},
                   {'table': 'genre_film_work', 'columns': ('film_work_id', 'genre_id', 'created_at', 'id')}
                   ]

    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)
    for table in list_tables:
        postgres_saver.truncate_table(table['table'])
        sqlite_loader.load_from_sqlite(table['table'], table['columns'])


if __name__ == "__main__":
    dsl = {
        'dbname': os.environ.get('dbname', 'movies_database'),
        'user': os.environ.get('user', 'postgres'),
        'password': os.environ.get('password', 'postgres'),
        'host': os.environ.get('host', '127.0.0.1'),
        'port': os.environ.get('port', 5432),
    }
    try:
        with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(
            **dsl, cursor_factory=DictCursor
        ) as pg_conn:
            load_from_sqlite(sqlite_conn, pg_conn)
    except (Exception, psycopg2.OperationalError) as err:
        print('Error: ', err)
