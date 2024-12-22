import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

# Load environment variables from .env
load_dotenv()

class DatabaseConn:
    _engine = None
    _SessionLocal = None

    @staticmethod
    def _get_database_url():
        """Construct the database URL from .env variables."""
        username = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        database = os.getenv("DB_NAME")
        return f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"

    @classmethod
    def get_engine(cls):
        """Return the SQLAlchemy engine (singleton)."""
        if cls._engine is None:
            database_url = cls._get_database_url()
            cls._engine = create_engine(database_url, echo=False)
        return cls._engine

    @classmethod
    def get_session(cls):
        """Return a new SQLAlchemy session.
            can be called using the engine created and will return a 
            new session using the existing connection (engine)
            """
        if cls._SessionLocal is None:
            cls._SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=cls.get_engine())
        return cls._SessionLocal()

    @classmethod
    def test_connection(cls):
        """Test the database connection."""
        try:
            with cls.get_engine().connect() as conn:
                result = conn.execute(text("SELECT version();"))
                print("PostgreSQL Version:", result.scalar())
        except Exception as e:
            print("Error connecting to the database:", e)
