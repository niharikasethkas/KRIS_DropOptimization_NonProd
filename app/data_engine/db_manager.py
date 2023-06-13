""" Script that details the database manager used """
import os
from datetime import date
from urllib import parse

import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, exc

from app import config
from app.helpers import utils


class DBManager:
    """ Database manager """

    def __init__(self, database):
        """ Constructor """
        self.database = database
        self.db_config_settings = self.get_settings(self.database)
        self.engine = self.generate_connection()
        # self.logger = utils.setup_logging("db_manager", config.SHARED_LOGGING_PARAMETERS)

    @staticmethod
    def get_settings(database):
        """ Function to get database settings """
        settings = {}

        # if database == 'redshift':
        #     settings = {
        #         "hostname": config.RS_DB_HOSTNAME,
        #         "schema": config.RS_DB_SCHEMA,
        #         "database": config.RS_DB_DATABASE,
        #         "user": parse.quote_plus(config.RS_DB_USER_NAME),
        #         "password": parse.quote_plus(os.environ["RS_DB_PASSWORD"])
        #     }
        if database == 'snowflake':
            settings = {
                "account": config.SF_DB_ACCOUNT,
                "user": config.SF_DB_USER,
                "password": parse.quote_plus(os.environ["SF_DB_PASSWORD"]),
                "schema": config.SF_DB_SCHEMA,
                "database": config.SF_DB_DATABASE,
                "warehouse": config.SF_DB_WAREHOUSE,
                "role": config.SF_DB_ROLE
            }

        return settings

    def generate_connection(self):
        """ Function to generate database connection """
        engine = None
        # if self.database == 'redshift':
        #     engine = create_engine("""postgresql+psycopg2://""" + self.db_config_settings["user"] + """:""" +
        #                            self.db_config_settings["password"] + """@""" + self.db_config_settings["hostname"] +
        #                            """:5439/""" + self.db_config_settings["database"])
        if self.database == 'snowflake':
            engine = create_engine(URL(
                account=self.db_config_settings['account'],
                user=self.db_config_settings['user'],
                password=self.db_config_settings['password'],
                database=self.db_config_settings['database'],
                warehouse=self.db_config_settings['warehouse'],
                role=self.db_config_settings['role']))

        return engine

    def pull_into_dataframe(self, query):
        """ This function takes in query as input and returns the dataframe
        Note: The query should be a select statement and not a create statement
        """
        df_out = pd.read_sql(query, self.engine)
        return df_out

    def insert_into_table(self, df_in, table_name):
        """
        This function takes in dataframe and table name as input parameters
        The dataframe is uploaded and inserted into table table_name

        """
        table_name = table_name.lower()
        # self.logger.info(f"Inserting into table: {table_name}")
        df_in.to_sql(name=table_name, schema=self.db_config_settings["schema"], con=self.engine,
                     index=False, if_exists='append', method='multi', chunksize=config.INSERT_CHUNK_SIZE)

    def create_table(self, table_name, query):
        """ Function to create table """
        # self.logger.info(f"Creating (Drop & Create) table: {table_name}")
        try:
            self.engine.execute(f"""DROP TABLE IF EXISTS {self.db_config_settings["schema"]}.{table_name}""")
            self.engine.execute(query)
            if self.database == 'redshift':
                self.engine.execute(f"""GRANT ALL ON {self.db_config_settings["schema"]}.{table_name}
                TO GROUP data_analytics_grp""")
            # self.logger.info("Table created successfully")
        except exc.SQLAlchemyError as error:
            print("Error")
            # self.logger.error(f"Exception: {error}")

    def create_view(self, view_name, query):
        """ Function to create table """
        # self.logger.info(f"Creating (Drop & Create) view: {view_name}")
        try:
            self.engine.execute(f"""DROP VIEW IF EXISTS {self.db_config_settings["schema"]}.{view_name}""")
            self.engine.execute(query)
            if self.database == 'redshift':
                self.engine.execute(f"""GRANT ALL ON {self.db_config_settings["schema"]}.{view_name}
                TO GROUP data_analytics_grp""")
            # self.logger.info("view created successfully")
        except exc.SQLAlchemyError as error:
            print("Error")
            # self.logger.error(f"Exception: {error}")

    def update_table_metadata(self, table_name, description):
        """ Function to update the metadata table """
        today = str(date.today())
        self.engine.execute(f"""DELETE FROM {self.db_config_settings["schema"]}.MD_metadata
        WHERE table_name='{table_name}'""")
        self.engine.execute(f"""INSERT INTO {self.db_config_settings["schema"]}.MD_metadata VALUES
        ('{table_name}', '{today}','{self.db_config_settings["user"]}','{description}')""")
        if self.database == 'redshift':
            self.engine.execute(f"""GRANT ALL ON {self.db_config_settings["schema"]}.{table_name}
            TO GROUP data_analytics_grp;""")
            self.engine.execute(f"""GRANT ALL ON {self.db_config_settings["schema"]}.{table_name}
            TO GROUP kdw_batch_grp;""")

    def drop_table(self, table_name):
        """ Function to drop a table"""
        # self.logger.info(f"Dropping table {table_name}")
        self.engine.execute(f"""DROP TABLE IF EXISTS  {self.db_config_settings["schema"]}.{table_name}""")

    def drop_view(self, view_name):
        """ Function to drop a table"""
        self.engine.execute(f"""DROP VIEW IF EXISTS {self.db_config_settings["schema"]}.{view_name}""")

    def execute_query(self, query):
        """ Execute the Query"""
        self.engine.execute(query)

    def execute_scalar(self, query, scalar):
        """ Function to execute scalar query """
        with self.engine.connect() as con:
            row = ''
            result = con.execute(query)
            if scalar:
                for row in result:
                    break
                output = row[0]
            else:
                output = [row[0] for row in result]
            return output
