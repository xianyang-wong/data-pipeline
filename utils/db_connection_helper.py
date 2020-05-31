import pandas.io.sql as sqlio
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import execute_batch
import pandas as pd


class rdbmsConnectionHelper(object):
    def __init__(self, host, port, user, password, database):

        self.__host=host
        self.__port = port
        self.__user = user
        self.__password = password
        self.__database = database

        self.__pool = ThreadedConnectionPool(1, 5,
                                             host=host,
                                             port=port,
                                             user=user,
                                             password=password,
                                             database=database)

    def check_table_exists(self, schema, table):
        conn = self.__pool.getconn()
        cur = conn.cursor()
        cur.execute(
            '''
            SELECT EXISTS(
                SELECT * 
                FROM information_schema.tables 
                WHERE table_schema=%(schema)s 
                AND table_name=%(table)s
                )
            ''', {'schema': schema, 'table': table})
        result = cur.fetchone()[0]
        self.__pool.putconn(conn)

        return result

    def create_table(self, schema, table, table_params):
        try:
            conn = self.__pool.getconn()
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute('''
                               CREATE TABLE %(schema)s.%(table)s (%(table_params)s);
                           ''', {'schema': schema, 'table': table, 'table_params': table_params})
        finally:
            if conn:
                self.__pool.putconn(conn)

    def execute_batch_insert(self, table, df):
        """
        batch insert for pandas dataframe
        :param table:
        :param df:
        :return:
        """
        df = df.where(pd.notnull(df), None)
        df_columns = list(df)
        # create (col1,col2,...)
        columns = ",".join(df_columns)
        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
        # create INSERT INTO table (columns) VALUES('%s',...)
        insert_query = "INSERT INTO {} ({}) {}".format(table, columns, values)
        insert_query = insert_query.replace("'NULL'", 'NULL')
        conn = self.__pool.getconn()
        try:
            with conn:
                with conn.cursor() as cursor:
                    execute_batch(cursor, insert_query, df.values)
        except Exception as e:
            print(e)
        finally:
            if conn:
                self.__pool.putconn(conn)

    def get_pandas_dataframe(self, sql, params=None):
        """
        return pandas dataframe using pandas IO
        :param sql:
        :param params:
        :return:
        """
        conn = self.__pool.getconn()
        try:
            with conn:
                df = sqlio.read_sql_query(sql=sql, con=conn, params=params)
                return df
        finally:
            if conn:
                self.__pool.putconn(conn)

    def get_raw_data(self, sql, params=None):
        """
        Return Data w/o pandas help
        :param sql:
        :param params:
        :return:
        """
        conn = self.__pool.getconn()
        try:
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    rows = cursor.fetchall()
                    data = []
                    for i in rows:
                        data.append(i)
                    return data
        finally:
            if conn:
                self.__pool.putconn(conn)

    def truncate_table(self, schema, table):

        try:
            conn = self.__pool.getconn()
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute('''
                        TRUNCATE TABLE %(schema)s.%(table)s;
                    ''', {'schema': schema, 'table': table})
        finally:
            if conn:
                self.__pool.putconn(conn)
