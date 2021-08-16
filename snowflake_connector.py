from sqlalchemy import create_engine
import snowflake.connector
import pandas as pd

import os
# from airflow.hooks.base_hook import BaseHook
from sqlalchemy.exc import OperationalError
import json


class SnoflakeAPI:
    def __init__(self):

        # credentials setup for local use
        ACCOUNT=os.environ.get('DB_ACCOUNT')
        USER=os.environ.get('USER_DB')
        warehouse=os.environ.get('DB_WAREHOUSE')
        DATABASE=os.environ.get('DB')
        authenticator='externalbrowser',

        self.con = snowflake.connector.connect(
        user=USER,
        account=ACCOUNT,
        authenticator='externalbrowser',
        warehouse=warehouse,
        database=DATABASE,
        )
    
    def __execute_snowflake_query__(self,query, with_cursor=False):
        cursor = self.con.cursor()
        try:
            cursor.execute(query)
            res = cursor.fetchall()
            if with_cursor:
                return (res, cursor)
            else:
                return res
        finally:
            cursor.close()

    def pandas_df_from_snowflake_query(self,query):
        result, cursor = self.__execute_snowflake_query__(query, with_cursor=True)
        headers = list(map(lambda t: t[0], cursor.description))
        df = pd.DataFrame(result)
        df.columns = headers
        return df
