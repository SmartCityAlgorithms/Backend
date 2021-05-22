import pandas as pd
from sshtunnel import SSHTunnelForwarder, BaseSSHTunnelForwarderError
from sqlalchemy import create_engine
from sqlalchemy.schema import MetaData
from sqlalchemy.exc import InternalError, \
    OperationalError, \
    ProgrammingError
import config
from time import strftime


class DB:
    def __init__(self, db_data: dict, logger):
        self.db_data = db_data
        self.logger = logger
        self.error = False

        if not self.db_data:
            self.error = 'There is on required data in "db_data" for DB connecting'
        elif self.db_data.get('ssh_host'):
            server, self.error = self.__server_connect()
            if not self.error:
                self.server = server
                self.mysql_link = f"mysql+pymysql://{self.db_data['mysql_user']}:{self.db_data['mysql_password']}@{self.local_host}:{self.local_port}/{self.db_data['mysql_db']}?charset=utf8mb4"
        else:
            self.server = None
            self.mysql_link = f"mysql+pymysql://{self.db_data['mysql_user']}:{self.db_data['mysql_password']}@{self.db_data['mysql_host']}:{self.db_data['mysql_port']}/{self.db_data['mysql_db']}?charset=utf8mb4"

        if not self.error:
            self.engine = create_engine(self.mysql_link, pool_pre_ping=True)
            conn, self.error = self.__mysql_connect()
            if not self.error:
                self.conn = conn
                self.meta = MetaData()
                self.meta.reflect(bind=self.engine)

    def __server_connect(self):
        self.local_host, self.local_port = '127.0.0.1', 33069
        try:
            server = SSHTunnelForwarder(
                (self.db_data['ssh_host'], self.db_data['ssh_port']),
                ssh_username=self.db_data['ssh_user'],
                ssh_pkey=self.db_data['ssh_key'],  # or ssh_password.
                remote_bind_address=(self.db_data['mysql_host'], self.db_data['mysql_port']),
                local_bind_address=(self.local_host, self.local_port))
        except ValueError as e:
            self.error = str(e)
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            self.logger.warning(f'{dt} Exception: {str(e)}')
            return False, self.error

        try:
            server.start()
        except BaseSSHTunnelForwarderError as e:
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            self.logger.warning(f'{dt} Exception: {str(e)}')
            self.error = str(e)
            return False, self.error

        return server, False

    def __mysql_connect(self):
        try:
            conn = self.engine.connect()
        except OperationalError as e:
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            self.logger.warning(f'{dt} Exception: {str(e)}')
            return False, str(e)
        except RuntimeError as e:
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            self.logger.warning(f'{dt} Exception: {str(e)}')
            return False, str(e)
        return conn, False

    def __reconnect(self):
        self.error = False
        self.server = None
        if self.db_data.get('ssh_host'):
            server, self.error = self.__server_connect()
            if not self.error:
                self.server = server
                self.mysql_link = f"mysql+pymysql://{self.db_data['mysql_user']}:{self.db_data['mysql_password']}@{self.local_host}:{self.local_port}/{self.db_data['mysql_db']}?charset=utf8mb4"
        else:
            self.server = None
            self.mysql_link = f"mysql+pymysql://{self.db_data['mysql_user']}:{self.db_data['mysql_password']}@{self.db_data['mysql_host']}:{self.db_data['mysql_port']}/{self.db_data['mysql_db']}?charset=utf8mb4"

        if not self.error:
            self.engine = create_engine(self.mysql_link, pool_pre_ping=True)
            conn, self.error = self.__mysql_connect()
            if not self.error:
                self.conn = conn
                self.meta = MetaData()
                self.meta.reflect(bind=self.engine)

    def insert_row(self, query_dict: dict):
        if self.error:
            return False, 'Some error with connection to database'
        jobs_table = self.meta.tables[config.jobs_table]

        try:
            results = self.conn.execute(jobs_table.insert(), query_dict)
        except ProgrammingError as e:
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            self.logger.warning(f'{dt} Exception: {str(e)}')
            self.error = str(e)
            return False, self.error
        except InternalError as e:
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            self.logger.warning(f'{dt} Exception: {str(e)}')
            self.error = str(e)
            return False, self.error
        except TypeError as e:
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            self.logger.warning(f'{dt} Exception: {str(e)}')
            self.error = str(e)
            return False, self.error

        if results.is_insert:
            return True, results.lastrowid

        dt = strftime("[%Y-%b-%d %H:%M:%S]")
        self.logger.warning(f"{dt} Exception: {str('Some error with adding')}")
        self.error = str('Some error with adding')
        return False, self.error

    def update_row(self, job_id, user, job_data: dict):
        if self.error:
            return False, 'Some error with connection to database'
        jobs_table = self.meta.tables[config.jobs_table]

        try:
            if user == config.admin:
                results = self.conn.execute(
                    jobs_table.update().where(jobs_table.c.id == job_id).values(job_data)
                )
            else:
                results = self.conn.execute(
                    jobs_table.update().where(jobs_table.c.id == job_id).where(
                        jobs_table.c.user == user
                    ).values(job_data)
                )
        except ProgrammingError as e:
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            self.logger.warning(f'{dt} Exception: {str(e)}')
            self.error = str(e)
            return False, self.error
        except InternalError as e:
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            self.logger.warning(f'{dt} Exception: {str(e)}')
            self.error = str(e)
            return False, self.error
        except TypeError as e:
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            self.logger.warning(f'{dt} Exception: {str(e)}')
            self.error = str(e)
            return False, self.error
        except OperationalError:
            self.__reconnect()
            try:
                if user == config.admin:
                    results = self.conn.execute(
                        jobs_table.update().where(jobs_table.c.id == job_id).values(job_data)
                    )
                else:
                    results = self.conn.execute(
                        jobs_table.update().where(jobs_table.c.id == job_id).where(
                            jobs_table.c.user == user
                        ).values(job_data)
                    )
            except ProgrammingError as e:
                dt = strftime("[%Y-%b-%d %H:%M:%S]")
                self.logger.warning(f'{dt} Exception: {str(e)}')
                self.error = str(e)
                return False, self.error
            except InternalError as e:
                dt = strftime("[%Y-%b-%d %H:%M:%S]")
                self.logger.warning(f'{dt} Exception: {str(e)}')
                self.error = str(e)
                return False, self.error
            except TypeError as e:
                dt = strftime("[%Y-%b-%d %H:%M:%S]")
                self.logger.warning(f'{dt} Exception: {str(e)}')
                self.error = str(e)
                return False, self.error
            except OperationalError as e:
                dt = strftime("[%Y-%b-%d %H:%M:%S]")
                self.logger.warning(f'{dt} Exception: {str(e)}')
                self.error = str(e)
                return False, self.error

        return True, results.rowcount

    def get_data(self, table=None, query=None):
        if self.error:
            return False, self.error

        status, self.error = self.__create_df(table, query)
        if status:
            return True, self.__df
        else:
            return False, self.error

    def __create_df(self, table=None, query=None):
        if not query and not table:
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            self.logger.warning(f'{dt} Exception: There are no query or required table for query')
            return False, 'There are no query or required table for query'
        elif not query:
            query = f"select * from {self.db_data['mysql_db']}.{table}"

        results = False
        try:
            if not table:
                results = self.conn.execute(query)
            else:
                self.__df = pd.read_sql(query, self.conn)
        except ProgrammingError as e:
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            self.logger.warning(f'{dt} Exception: {str(e)}')
            self.error = str(e)
            return False, self.error
        except InternalError as e:
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            self.logger.warning(f'{dt} Exception: {str(e)}')
            self.error = str(e)
            return False, self.error
        except TypeError as e:
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            self.logger.warning(f'{dt} Exception: {str(e)}')
            self.error = str(e)
            return False, self.error

        if results:
            self.__df = pd.DataFrame(results.fetchall(), columns=results.keys())

        return True, False

    def remove_row(self, job_id, user):
        if self.error:
            return False, 'Some error with connection to database'
        jobs_table = self.meta.tables[config.jobs_table]
        status_lst = ['success', 'error']

        try:
            if user == config.admin:
                results = self.conn.execute(
                    jobs_table.delete().where(jobs_table.c.id == job_id).where(jobs_table.c.status.in_(status_lst))
                )
            else:
                results = self.conn.execute(
                    jobs_table.delete().where(jobs_table.c.id == job_id).where(jobs_table.c.status.in_(status_lst)
                                                                               ).where(jobs_table.c.user == user)
                )
        except ProgrammingError as e:
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            self.logger.warning(f'{dt} Exception: {str(e)}')
            self.error = str(e)
            return False, self.error
        except InternalError as e:
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            self.logger.warning(f'{dt} Exception: {str(e)}')
            self.error = str(e)
            return False, self.error
        except TypeError as e:
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            self.logger.warning(f'{dt} Exception: {str(e)}')
            self.error = str(e)
            return False, self.error
        if results.rowcount > 0:
            return True, True

        dt = strftime("[%Y-%b-%d %H:%M:%S]")
        self.logger.warning(f"{dt} Exception: {str('Empty result of removing')}")
        self.error = str('Empty result of removing')
        return False, 'Empty result of removing'

    def close_connection(self):
        if self.error:
            return False

        self.conn.close()
        self.engine.dispose()
        if self.server:
            self.server.stop()
        return True

    def save_to_mysql(self, df, table, if_exists='replace'):
        if self.error:
            return False, 'some error in previous steps'

        try:
            df.to_sql(name=table, con=self.conn, if_exists=if_exists, index=False)
        except InternalError as e:
            self.error = str(e) + e.statement
            self.conn.close()
            dt = strftime("[%Y-%b-%d %H:%M:%S]")
            self.logger.warning(f'{dt} Exception: {str(self.error)}')
            return False, self.error

        return True, self.error


if __name__ == '__main__':
    mydb_0 = DB(config.db_data)
    df1 = mydb_0.get_data(table=config.report_table)
    print(df1.head(3))
    status = mydb_0.close_connection()

    mydb = DB(config.app_db_data)
    query1 = """
    select type_operation_id,
     type_operation_text,
     comment,
     type_coupon,
     total as 'Total',
     created_at as 'Created at'
    from invoice_history_has_customer_coupons
    where type_operation_id <> 3 
     and type_operation_id <> 7 
     and created_at >= '2020-08-24'
          and created_at >= '2020-08-24 00:00:00'
     and created_at <= '2020-09-06 23:59:59'
    """
    df2 = mydb.get_data(query=query1)
    print(df2.head(3))
    status = mydb.close_connection()