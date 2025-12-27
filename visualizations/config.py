import pandas as pd
from sqlalchemy import create_engine

DATABASE_URL = "postgresql+psycopg2://analytics_user:analytics_password@localhost:5432/finance_analytics"


def get_engine():
    return create_engine(DATABASE_URL)


def fetch_data(query: str) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql_query(query, conn)
