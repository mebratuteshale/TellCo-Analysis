# connect_to_db.py

import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Database connection details
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT"))
class DBConnection:
    def __init__(self):
        self.db_host=DB_HOST
        self.db_name=DB_NAME
        self.db_user=DB_USER
        self.db_pwd=DB_PASSWORD
        self.db_port=DB_PORT

    def getConnection():
        try:
            # Connect to the database
            connection = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT
            )
            print("Connection successful!")
        except Exception as e:
            print("An error occurred while connecting to the database:", e)
        finally:
            if connection:
                print("connection obj returned")
                return connection
            else:
                # print("connection obj returned")
                return "ERROR Connection to Database"
            
