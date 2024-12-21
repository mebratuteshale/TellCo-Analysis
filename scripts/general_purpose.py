# General purpose scripts goes here
import pandas as pd
import os 
import sys
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.sql import text

current_dir=os.getcwd()                     #current working directory
parent_dir=os.path.dirname(current_dir)     # Get the parent directory
sys.path.append(parent_dir)

from database_connection import DatabaseConn

def fetch_data_as_dataframe(query: str):
    """
    Fetch data from the database and load it into a pandas DataFrame.

    Args:
        query (str): The SQL query to execute.

    Returns:
        pd.DataFrame: DataFrame containing the query results.
    """
    try:
        # Get the engine and execute the query
        engine = DatabaseConn.get_engine()
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print("Error fetching data:", e)
        return pd.DataFrame()
    


def main():
    # Example SQL query to fetch data
    query = "SELECT * FROM xdr_data LIMIT 10;"
    
    # Fetch data and create a DataFrame
    df = fetch_data_as_dataframe(query)

    # Print the DataFrame
    if not df.empty:
        print("Data fetched successfully:")
        print(df)
    else:
        print("No data fetched or an error occurred.")

if __name__ == "__main__":
    main()

