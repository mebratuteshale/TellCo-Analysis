# General purpose scripts goes here
import pandas as pd
import os 
import sys
import psycopg2
from sqlalchemy import create_engine

current_dir=os.getcwd()                     #current working directory
parent_dir=os.path.dirname(current_dir)     # Get the parent directory
sys.path.append(parent_dir)

from connect_to_db import DBConnection

dbConn=DBConnection()

# Create a connection string

# connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
connection_string = f"postgresql+psycopg2://{dbConn.db_user}:{dbConn.db_pwd}@{dbConn.db_host}:{dbConn.db_port}/{dbConn.db_name}"
def load_data(query):
    try:
        engine=create_engine(connection_string)
        # connection = psycopg2.connect(
        #             host=dbConn.db_host,
        #             database=dbConn.db_name,
        #             user=dbConn.db_user,
        #             password=dbConn.db_pwd,
        #             port=dbConn.db_port
        #         )
        # data = pd.read_sql_query(query,connection)
        # connection.close()

        df_telecom=pd.read_sql_query(query,engine)
        df_telecom.head()
        return df_telecom
    except Exception as ex:        
        print(f"Error occured : {ex}")

df_teleco = load_data("select * from xdr_data")
df_teleco.head()