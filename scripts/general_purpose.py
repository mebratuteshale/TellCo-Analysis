# General purpose scripts goes here
import pandas as pd
import os 
import sys
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.sql import text

# current_dir=os.getcwd()                     #current working directory
# parent_dir=os.path.dirname(current_dir)     # Get the parent directory
# sys.path.append(parent_dir)

from scripts.database_connection import DatabaseConn

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
    
def group_dataframe(dataframe,groupbyColName):
    df_grouped = (dataframe[groupbyColName])
    return df_grouped


def explore_dataframe(dataFrame):
    print(f"*************** Shape: {dataFrame.shape} ***************")
    print("**************** Info *****************************")
    print(dataFrame.info())


def data_cleaning(dataFrame, colList):
    print("Dataset Info :",dataFrame.info())
    dupe_rows=dataFrame[dataFrame.duplicated()]
    if dupe_rows.empty:
        print("No dupes")        # print duplicated rows
    else:
        print("Dupe rows :\n",dupe_rows)
        dataFrame.drop_duplicates()
    for col in colList:
        dataFrame.dropna(subset=[col], inplace=True)

    


def main():
    # SQL query to fetch data
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

