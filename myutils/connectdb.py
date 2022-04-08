# coding: utf-8
"""Database connected functions.

Created on 2021/03/16 by Jerry.Ko

"""
import os
import pandas as pd
import pyodbc
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine
from google.cloud import bigquery as bq


class ConnectedMSSQL:

    def __init__(self, server, database, username):
        load_dotenv()
        users = json.loads(os.getenv('USERS'))
        
        self.server = server 
        self.database = database  
        self.username = username 
        # self.password = users[username] 
        pyodbc.pooling = False # https://docs.sqlalchemy.org/en/14/dialects/mssql.html#pyodbc-pooling-connection-close-behavior
        self.engine = create_engine(
            "mssql+pyodbc://{0}:{1}@{2}/{3}?driver=ODBC Driver 17 for SQL Server".format(
                self.username, users[username], self.server, self.database
            ), 
            fast_executemany=True
        )

    def read_data(self, query: str):
        """yoxi_ds MSSQL connected via pyodbc and pandas.
        Args:
          query: str. select query of SQL.
        Return:
          df: dataframe of pnadas. 
        """
        # with self.engine.connect().execution_options(autocommit=True) as conn:
        df = pd.read_sql(query, self.engine)
        return df

    #insert data to MSSQL
    def insert_into_data(self, df, table: str, dtypes=None):
        """from pandas df insert into data to mssql DB.
        Args:
          df: dataframe of pandas. update data, type is frame.
          table: str. name of SQL table.
          dtype: dict. Specifying the datatype for columns.
        Return:
          None
        """
        df.to_sql(table, self.engine, if_exists='append', index=False, dtype=dtypes)

    def execute_sql(self, stmt: str):
        """直接使用 SQL 語句對 DB 進行操作。如 truncate tabel, update table 等指令。
        Args:
          stmt: str. SQL statement, can used delete or truncate
        Return:
          None
        """
        from sqlalchemy.sql import text
        with self.engine.connect().execution_options(autocommit=True) as con:
            con.execute(text(stmt))
        




class ConnectedBigQuery:
    """未完成，要新增 update 方法或 excute 語法"""
    def __init__(self, server, token_path):
        self.server = server 
        self.token_path = token_path 


    def read_data(self, query: str):
        """讀取 GCP BigQuery 資料。
        Args:
          query: str. bigquery 的 query 語句。
        Return:
          df: 
        """
        # 設定token環境變數
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.token_path
        
        client = bq.Client()
        df = client.query(query).to_dataframe()
        client.close()
        
        return df

    def insert_into_bigquery(self, df, BQ_table_id: str, write_type='WRITE_TRUNCATE'):
        """使用 pandas dataframe 上傳資料到 bigquery 指定的 dataset, table
        Args:
          df: pandas dataframe. 要上傳的 df。
          BQ_table_id: string. bigquery的dataset.table。
          write_type: string. 資料存在時的上傳方式，預設為 `WRITE_TRUNCATE`。
              另有 `WRITE_APPEND` 添加以及 `WRITE_EMPTY` 報錯。
        Return: 
          None
        """
        import os
        from google.cloud import bigquery as bq
        # 設定token環境變數
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.token_path
        client = bq.Client()
    #     table_schema = [bq.SchemaField(col, "STRING") for col in df.columns]
        job_config = bq.LoadJobConfig(write_disposition=write_type)
        
        job = client.load_table_from_dataframe(df, BQ_table_id, job_config=job_config)
        client.close()
